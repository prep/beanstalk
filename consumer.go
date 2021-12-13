package beanstalk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Consumer maintains a pool of connections and allows workers to reserve jobs
// on those connections.
type Consumer struct {
	uris     []string
	tubes    []string
	config   Config
	reserveC chan chan *Job
	wg       sync.WaitGroup
}

// NewConsumer returns a new Consumer or returns an error if the uris is not valid or empty.
func NewConsumer(uris, tubes []string, config Config) (*Consumer, error) {
	if !config.IgnoreURIValidation {
		if err := ValidURIs(uris); err != nil {
			return nil, err
		}
	}

	config = config.normalize()

	return &Consumer{
		uris:     uris,
		tubes:    tubes,
		config:   config,
		reserveC: make(chan chan *Job, config.NumGoroutines),
	}, nil
}

// Receive calls fn for each job it can reserve.
func (consumer *Consumer) Receive(ctx context.Context, fn func(ctx context.Context, job *Job)) {
	// Spin up the connections to the beanstalk servers.
	for _, uri := range multiply(consumer.uris, consumer.config.Multiply) {
		go maintainConn(ctx, uri, consumer.config, connHandler{
			setup:  consumer.watchTubes,
			handle: consumer.reserveJobs,
		})
	}

	// Spin up the workers in their own goroutine.
	for i := 0; i < consumer.config.NumGoroutines; i++ {
		consumer.wg.Add(1)

		go func() {
			defer consumer.wg.Done()
			consumer.worker(ctx, fn)
		}()
	}

	// Wait for all the workers to finish before returning.
	consumer.wg.Wait()
}

// worker calls fn for every job it can reserve.
func (consumer *Consumer) worker(ctx context.Context, fn func(ctx context.Context, job *Job)) {
	jobC := make(chan *Job)

	for {
		// Add a reserve request to the reserve channel.
		consumer.reserveC <- jobC

		select {
		// If a reserved job comes in, pass it to fn.
		case job := <-jobC:
			func() {
				defer job.Release(context.Background())
				fn(context.Background(), job)
			}()

		// Stop if the context was cancelled.
		case <-ctx.Done():
			return
		}
	}
}

// watchTubes watches the requested tubes.
func (consumer *Consumer) watchTubes(ctx context.Context, conn *Conn) error {
	if len(consumer.tubes) == 0 {
		return nil
	}

	// Watch all the requested tubes.
	for _, tube := range consumer.tubes {
		if err := conn.Watch(ctx, tube); err != nil {
			return fmt.Errorf("error watching tube: %s: %w", tube, err)
		}
	}

	// Ignore the default tube, unless it was explicitly requested.
	if !includes(consumer.tubes, "default") {
		if err := conn.Ignore(ctx, "default"); err != nil {
			return fmt.Errorf("error ignoring default tube: %w", err)
		}
	}

	return nil
}

// reserveJobs is responsible for reserving jobs on demand and pass them back
// to the worker method that will call its worker function with it.
func (consumer *Consumer) reserveJobs(ctx context.Context, conn *Conn) error {
	var err error

	// If the return error is nil, then the context was cancelled. However,
	// reserved jobs on this connection might still need to finish up so wait for
	// the workers to finish before returning (and thus closing) the connection.
	defer func() {
		if err == nil {
			consumer.wg.Wait()
		}
	}()

	var job *Job
	var jobC chan *Job
	for {
		// Wait for a reserve request from a worker to come in.
		select {
		case jobC = <-consumer.reserveC:
		case <-ctx.Done():
			return nil
		}

		// Attempt to reserve a job.
		if job, err = conn.reserveWithTimeout(ctx, 0); job != nil {
			select {
			// Return the job to the worker and wait for another reserve request.
			case jobC <- job:
				continue

			// Release the job and stop if the context was cancelled.
			case <-ctx.Done():
				if err = job.Release(context.Background()); err != nil {
					consumer.config.ErrorFunc(err, "Unable to release job after context was cancelled")
				}

				return nil
			}
		}

		// Put the reserve request back and return if an error occurred.
		consumer.reserveC <- jobC
		if err != nil {
			return err
		}

		// The watched tubes are empty, so wait a bit before reserving again.
		select {
		case <-time.After(consumer.config.ReserveTimeout):
		case <-ctx.Done():
			return nil
		}
	}
}
