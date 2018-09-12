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

	tubes []string

	// This is used to close this consumer.
	close     chan struct{}
	closeOnce sync.Once

	// This is used to keep track of the paused state.
	isPaused bool
	pause    chan bool
	mu       sync.Mutex
}

// NewConsumer connects to the beanstalk server that's referenced in URI and
// returns a Consumer.
func NewConsumer(URI string, tubes []string, config Config) (*Consumer, error) {
	config = config.normalize()

	conn, err := Dial(URI, config)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		C:        config.jobC,
		tubes:    tubes,
		close:    make(chan struct{}),
		pause:    make(chan bool, 1),
		isPaused: true,
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
	case consumer.pause <- false:
	case <-consumer.pause:
		consumer.pause <- false
	case <-consumer.close:
	}
}

// Pause this consumer.
func (consumer *Consumer) Pause() {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	select {
	case consumer.pause <- true:
	case <-consumer.pause:
		consumer.pause <- true
	case <-consumer.close:
	}
}

func (consumer *Consumer) setupConnection(conn *Conn, config Config) error {
	// If no tubes were specified, stick to the default one.
	if len(consumer.tubes) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, tube := range consumer.tubes {
		if err := conn.Watch(ctx, tube); err != nil {
			return fmt.Errorf("error watching tube: %s: %s", tube, err)
		}
	}

	if !contains(consumer.tubes, "default") {
		if err := conn.Ignore(ctx, "default"); err != nil {
			return fmt.Errorf("error ignoring default tube: %s", err)
		}
	}

	return nil
}

// handleIO is responsible for reserving jobs on the connection and offering
// them up to a listener on C.
func (consumer *Consumer) handleIO(conn *Conn, config Config) (err error) {
	var job *Job
	var jobC chan<- *Job

	// reserveTimeout is used to wait between reserve calls.
	reserveTimeout := time.NewTimer(0)
	reserveTimeout.Stop()

	// releaseTimeout is used to release a reserved job back before it got claimed.
	releaseTimeout := time.NewTimer(time.Second)
	releaseTimeout.Stop()

	// releaseJob releases a currently reserved job.
	releaseJob := func() error {
		if job != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = job.Release(ctx)
			cancel()

			job, jobC = nil, nil
		}

		return err
	}

	// reserveJob reserves a job unless the connection is paused, or a job has
	// been reserved already.
	reserveJob := func() error {
		if job == nil && !consumer.isPaused {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			job, err = conn.ReserveWithTimeout(ctx, 0)
			cancel()

			switch {
			case err != nil:
				return err
			// Job reserved, so start the release timer.
			case job != nil:
				jobC = config.jobC
				releaseTimeout.Reset(config.ReleaseTimeout)
			// No job reserved, so try again in a while.
			default:
				reserveTimeout.Reset(config.ReserveTimeout)
			}
		}

		return nil
	}

	for {
		select {
		// Offer up a reserved job.
		case jobC <- job:
			job, jobC = nil, nil
			releaseTimeout.Stop()

			// Immediately try to reserve a new job.
			if err = reserveJob(); err != nil {
				return err
			}

		// Try to reserve a new job.
		case <-reserveTimeout.C:
			if err = reserveJob(); err != nil {
				return err
			}

		// Release the reserved job back, after having claimed it for a while.
		case <-releaseTimeout.C:
			if err = releaseJob(); err != nil {
				return err
			}

		// Pause or unpause this consumer.
		case consumer.isPaused = <-consumer.pause:
			if consumer.isPaused {
				if err = releaseJob(); err != nil {
					return err
				}

				releaseTimeout.Stop()
			} else {
				reserveTimeout.Reset(0)
			}

		// If the connection closed, return to trigger a reconnect.
		case err = <-conn.Closed:
			return err

		// Exit when this consumer is closing down.
		case <-consumer.close:
			return releaseJob()
		}
	}
}
