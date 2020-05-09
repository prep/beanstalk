package beanstalk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Consumer maintains a pool of connections to beanstalk servers on which it
// reserves jobs.
type Consumer struct {
	uris     []string
	tubes    []string
	config   Config
	reserveC chan chan *Job
	wg       sync.WaitGroup
}

// NewConsumer returns a new Consumer.
func NewConsumer(uris []string, tubes []string, config Config) (*Consumer, error) {
	if err := validURIs(uris); err != nil {
		return nil, err
	}

	return &Consumer{
		uris:     uris,
		tubes:    tubes,
		config:   config.normalize(),
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

	// Spin up the worker goroutines that call fn for every reserved job.
	for i := 0; i < consumer.config.NumGoroutines; i++ {
		consumer.wg.Add(1)

		go func() {
			defer consumer.wg.Done()
			consumer.worker(ctx, fn)
		}()
	}

	// Wait for all the reserving goroutines to finish before returning.
	consumer.wg.Wait()
}

// worker issues async reserve requests and when successful, calls fn with the
// reserved job.
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

		// Stop if the context got cancelled.
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
			return fmt.Errorf("error watching tube: %s: %s", tube, err)
		}
	}

	// Ignore the default tube, unless it was explicitly requested.
	if !includes(consumer.tubes, "default") {
		if err := conn.Ignore(ctx, "default"); err != nil {
			return fmt.Errorf("error ignoring default tube: %s", err)
		}
	}

	return nil
}

// reserveJobs is responsible for reserving jobs on demand and pass them back
// to the goroutine in Receive that will call its fn with it.
func (consumer *Consumer) reserveJobs(ctx context.Context, conn *Conn) error {
	var jobC chan *Job
	var job *Job
	var err error

	// If the return error is nil, then wait for the goroutines spun up by
	// Receive to finish before returning. This is done because returning closes
	// the TCP connection to the beanstalk server and there might be jobs left
	// who need it to finish up.
	defer func() {
		if err == nil {
			consumer.wg.Wait()
		}
	}()

	for {
		// Wait for a reserve request to come in.
		select {
		case jobC = <-consumer.reserveC:
		case <-ctx.Done():
			return nil
		}

		// Attempt to reserve a job.
		if job, err = conn.ReserveWithTimeout(ctx, 0); job != nil {
			select {
			// Return the job to the worker.
			case jobC <- job:
				continue
			// Release the job and stop if the context got cancelled.
			case <-ctx.Done():
				if err = job.Release(ctx); err != nil {
					consumer.config.ErrorFunc(err, "Unable to release job back")
				}

				return nil
			}
		}

		// Put the reserve request back and return if an error occurred.
		consumer.reserveC <- jobC
		if err != nil {
			return err
		}

		// No reserved job and no error, so wait it bit before trying to reserve
		// again.
		select {
		case <-time.After(consumer.config.ReserveTimeout):
		case <-ctx.Done():
			return nil
		}
	}
}
