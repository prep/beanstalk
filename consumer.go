package beanstalk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Consumer maintains a connnection to a beanstalk server and offers up jobs
// on its exposed jobs channel. When it gets disconnected, it automatically
// tries to reconnect.
type Consumer struct {
	// C offers up reserved jobs.
	C <-chan *Job

	tubes     []string
	config    Config
	isPaused  bool
	pause     chan bool
	close     chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
}

// NewConsumer connects to the beanstalk server that's referenced in URI and
// returns a Consumer.
func NewConsumer(uri string, tubes []string, config Config) (*Consumer, error) {
	config = config.normalize()

	conn, err := Dial(uri, config)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		C:        config.jobC,
		tubes:    tubes,
		config:   config,
		isPaused: true,
		pause:    make(chan bool, 1),
		close:    make(chan struct{}),
	}

	keepConnected(consumer, conn, config, consumer.close)
	return consumer, nil
}

// Close this consumer's connection.
func (consumer *Consumer) Close() {
	consumer.closeOnce.Do(func() {
		close(consumer.close)
	})
}

// Play unpauses this customer.
func (consumer *Consumer) Play() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	select {
	case <-consumer.close:
	case consumer.pause <- false:
	case <-consumer.pause:
		consumer.pause <- false
	}
}

// Pause this consumer.
func (consumer *Consumer) Pause() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	select {
	case <-consumer.close:
	case consumer.pause <- true:
	case <-consumer.pause:
		consumer.pause <- true
	}
}

// Receive calls fn for each job it can reserve on this consumer.
func (consumer *Consumer) Receive(ctx context.Context, fn func(ctx context.Context, job *Job)) {
	var wg sync.WaitGroup
	wg.Add(consumer.config.NumGoroutines)

	for i := 0; i < consumer.config.NumGoroutines; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case job := <-consumer.C:
					fn(ctx, job)

				case <-ctx.Done():
					return
				case <-consumer.close:
					return
				}
			}
		}()
	}

	wg.Wait()
}

func (consumer *Consumer) setupConnection(conn *Conn, config Config) error {
	// If no tubes were specified, stick to the default one.
	if len(consumer.tubes) == 0 {
		return nil
	}

	return contextTimeoutFunc(3*time.Second, func(ctx context.Context) error {
		for _, tube := range consumer.tubes {
			if err := conn.Watch(ctx, tube); err != nil {
				return fmt.Errorf("error watching tube: %s: %s", tube, err)
			}
		}

		if !includes(consumer.tubes, "default") {
			if err := conn.Ignore(ctx, "default"); err != nil {
				return fmt.Errorf("error ignoring default tube: %s", err)
			}
		}

		return nil
	})
}

// handleIO is responsible for reserving jobs on the connection and offering
// them up to a listener on C.
func (consumer *Consumer) handleIO(conn *Conn, config Config) (err error) {
	var job *Job
	var jobC chan<- *Job

	// reserveTimeout is used to pause between reserve attempts when the last
	// attempt failed to reserve a job.
	reserveTimeout := time.NewTimer(0)
	if consumer.isPaused {
		reserveTimeout.Stop()
	}

	// releaseTimeout is used to release a reserved job back to prevent holding
	// a job too long when it doesn't get claimed in a reasonable amount of time.
	releaseTimeout := time.NewTimer(time.Second)
	releaseTimeout.Stop()

	// releaseJob releases the currently reserved job.
	releaseJob := func() error {
		if job == nil {
			return nil
		}

		releaseTimeout.Stop()
		err = contextTimeoutFunc(3*time.Second, job.Release)

		// Don't treat NOT_FOUND responses as a fatal error.
		if err == ErrNotFound {
			config.ErrorLog.Printf("Consumer could not release job %d: %s", job.ID, err)
			err = nil
		}

		job, jobC = nil, nil
		return err
	}

	// reserveJob reserves a job.
	reserveJob := func() error {
		// Don't do anything if the connection is paused, or a job was already
		// reserved.
		if consumer.isPaused || job != nil {
			return nil
		}

		// Attempt to reserve a job.
		err = contextTimeoutFunc(3*time.Second, func(ctx context.Context) error {
			job, err = conn.ReserveWithTimeout(ctx, 0)
			return err
		})

		switch {
		case err != nil:
			return err

		// Job reserved, so start the release timer so that the job isn't being
		// held for too long if it doesn't get claimed.
		case job != nil:
			jobC = config.jobC

			// Make sure the release timer isn't bigger than the TTR of the job.
			if job.TouchAfter() < config.ReleaseTimeout {
				releaseTimeout.Reset(job.TouchAfter())
			} else {
				releaseTimeout.Reset(config.ReleaseTimeout)
			}

		// No job reserved, so try again after a pause.
		default:
			reserveTimeout.Reset(config.ReserveTimeout)
		}

		return nil
	}

	for {
		select {
		// Offer up the reserved job.
		case jobC <- job:
			job, jobC = nil, nil
			releaseTimeout.Stop()

			// Immediately try to reserve a new job.
			reserveTimeout.Reset(0)

		// Try to reserve a new job.
		case <-reserveTimeout.C:
			if err = reserveJob(); err != nil {
				return err
			}

		// Release the reserved job back, after having held it for a while.
		case <-releaseTimeout.C:
			if err = releaseJob(); err != nil {
				return err
			}

			// Wait a bit before attempting another reserve. This gives other workers
			// time to pick the previously released job.
			reserveTimeout.Reset(config.ReserveTimeout)

		// Pause or unpause this consumer.
		case consumer.isPaused = <-consumer.pause:
			if consumer.isPaused {
				if err = releaseJob(); err != nil {
					return err
				}
			} else {
				reserveTimeout.Reset(0)
			}

		// Exit when this consumer is closing down.
		case <-consumer.close:
			return releaseJob()
		}
	}
}
