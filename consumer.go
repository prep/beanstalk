package beanstalk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

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
	// Spin up connections to the beanstalk servers.
	for i := 0; i < consumer.config.Multiply; i++ {
		for _, uri := range consumer.uris {
			go func(uri string) {
				maintainConn(ctx, uri, consumer.config, connHandler{
					setup:  consumer.watchTubes,
					handle: consumer.reserveJobs,
				})
			}(uri)
		}
	}

	// Spin up the goroutines that pass reserved jobs to fn.
	for i := 0; i < consumer.config.NumGoroutines; i++ {
		consumer.wg.Add(1)

		go func() {
			defer consumer.wg.Done()

			jobC := make(chan *Job)
			for {
				// Add a reserve request to the reserve channel.
				consumer.reserveC <- jobC

				select {
				// If a reserved job comes in, pass it to worker function.
				case job := <-jobC:
					func() {
						defer job.Release(context.Background())
						fn(context.Background(), job)
					}()

				// Stop if the context closes.
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Wait for all the reserving goroutines to finish before returning.
	consumer.wg.Wait()
}

// watchTubes makes sure the appropriate tubes are being watched.
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

// reserveJobs reserves jobs and passes them to fn.
func (consumer *Consumer) reserveJobs(ctx context.Context, conn *Conn) error {
	var jobC chan *Job

	for {
		// Wait for a reserve request to come in, or the context to cancel.
		select {
		case jobC = <-consumer.reserveC:
		// Wait for all the goroutines in Receive to finish before returning.
		case <-ctx.Done():
			consumer.wg.Wait()
			return nil
		}

		// Attempt to reserve a job.
		job, err := conn.ReserveWithTimeout(ctx, 0)
		switch {
		// Stop on error.
		case err != nil:
			return err
		// Return the reserved job to the goroutine in Receive.
		case job != nil:
			jobC <- job
			continue
		// Put the request back on the queue.
		default:
			consumer.reserveC <- jobC
		}

		// No job was reserved, so wait a bit before trying to reserve again.
		select {
		case <-time.After(consumer.config.ReserveTimeout):
		// Wait for all the goroutines in Receive to finish before returning.
		case <-ctx.Done():
			consumer.wg.Wait()
			return nil
		}
	}
}
