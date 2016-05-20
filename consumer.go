package beanstalk

import (
	"strings"
	"sync"
	"time"
)

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	tubes     []string
	jobC      chan<- *Job
	pause     chan bool
	stop      chan struct{}
	isPaused  bool
	isStopped bool
	options   *Options
	client    *Client
	sync.Mutex
}

// NewConsumer returns a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options *Options) *Consumer {
	if options == nil {
		options = DefaultOptions()
	}

	consumer := &Consumer{
		tubes:    tubes,
		jobC:     jobC,
		pause:    make(chan bool, 1),
		stop:     make(chan struct{}, 1),
		isPaused: true,
		options:  options,
	}

	go consumer.connectionManager(socket)
	return consumer
}

// Play allows this consumer to start reserving jobs. Returns true on success
// and false if this consumer was stopped.
func (consumer *Consumer) Play() bool {
	consumer.Lock()
	defer consumer.Unlock()

	if consumer.isStopped {
		return false
	}

	select {
	case <-consumer.pause:
	default:
	}

	consumer.pause <- false
	return true
}

// Pause stops this consumer from reserving jobs. Returns true on success and
// false if this consumer was stopped.
func (consumer *Consumer) Pause() bool {
	consumer.Lock()
	defer consumer.Unlock()

	if consumer.isStopped {
		return false
	}

	select {
	case <-consumer.pause:
	default:
	}

	consumer.pause <- true
	return true
}

// Stop this consumer. Returns true on success and false if this consumer was
// already stopped.
func (consumer *Consumer) Stop() bool {
	consumer.Lock()
	defer consumer.Unlock()

	if consumer.isStopped {
		return false
	}

	consumer.isStopped = true
	consumer.stop <- struct{}{}
	return true
}

// connectionManager is responsible for setting up a connection to the
// beanstalk server and wrapping it in a Client, which on success is passed
// to the clientManager function.
func (consumer *Consumer) connectionManager(socket string) {
	var (
		err     error
		options = consumer.options
	)

	// Start a new connection.
	newConnection, abortConnect := Connect(socket, consumer.options)

	for {
		select {
		// This case triggers whenever a new connection was established.
		case conn := <-newConnection:
			client := NewClient(conn, options)

			options.LogInfo("Watching tubes: %s", strings.Join(consumer.tubes, ", "))
			for _, tube := range consumer.tubes {
				if err = client.Watch(tube); err != nil {
					options.LogError("%s: Error watching tube: %s", tube, err)
					break
				}
			}

			if err == nil && !includesString(consumer.tubes, "default") {
				if err := client.Ignore("default"); err != nil {
					options.LogError("default: Unable to ignore tube: %s", err)
				}
			}

			if err := consumer.clientManager(client); err != nil {
				newConnection, abortConnect = Connect(socket, consumer.options)
			} else {
				return
			}

		// Keep track of the pause state.
		case consumer.isPaused = <-consumer.pause:

		// Abort this connection and stop this consumer all together when the
		// stop signal was received.
		case <-consumer.stop:
			abortConnect <- struct{}{}
			return
		}
	}
}

func (consumer *Consumer) clientManager(client *Client) (err error) {
	var (
		job          *Job
		jobOffer     chan<- *Job
		jobCommandC  = make(chan *JobCommand)
		jobsOutThere int
	)

	defer func() {
		if job != nil {
			client.Release(job, job.Priority, 0)
		}

		client.Close()
		close(jobCommandC)
	}()

	// This is used to pause the select-statement for a bit when the job queue
	// is full or when "reserve-with-timeout 0" yields no job.
	timeout := time.NewTimer(time.Second)
	timeout.Stop()

	// This timer is used to trigger a touch event for a pending job.
	touch := time.NewTimer(time.Second)
	touch.Stop()

	for {
		// Attempt to reserve a job if the state allows for it.
		if job == nil && !consumer.isPaused {
			if jobsOutThere == 0 {
				job, err = client.Reserve(consumer.options.ReserveTimeout)
			} else {
				job, err = client.Reserve(0)
			}

			switch {
			case err == ErrDraining:
				timeout.Reset(time.Minute)
			case err == ErrDeadlineSoon:
				timeout.Reset(time.Second)
			case err != nil:
				consumer.options.LogError("Error reserving job: %s", err)
				return err

			// A new job was reserved.
			case job != nil:
				job.Finish = jobCommandC
				jobOffer = consumer.jobC
				touch.Reset(job.TouchAfter())

			// With jobs out there and no successful reserve, wait a bit before
			// attempting another reserve.
			case jobsOutThere != 0:
				timeout.Reset(time.Second)

			// No job reserved and no jobs out there, perform another reserve almost
			// immediately.
			default:
				timeout.Reset(0)
			}
		} else {
			timeout.Reset(time.Second)
		}

		select {
		// Offer the job up on the shared jobs channel.
		case jobOffer <- job:
			job, jobOffer = nil, nil
			jobsOutThere++
			touch.Stop()

		// Wait a bit before trying to reserve a job again, or just fall through.
		case <-timeout.C:

		// Touch the pending job to make sure it doesn't expire.
		case <-touch.C:
			if job != nil {
				if err := client.Touch(job); err != nil {
					consumer.options.LogError("Unable to touch job %d: %s", job.ID, err)

					if err != ErrNotFound {
						return err
					}
				}

				touch.Reset(job.TouchAfter())
			}

		// Bury, delete or release a reserved job.
		case req := <-jobCommandC:
			switch req.Command {
			case Bury:
				err = client.Bury(req.Job, req.Priority)
			case Delete:
				err = client.Delete(req.Job)
			case Release:
				err = client.Release(req.Job, req.Priority, req.Delay)
			}

			jobsOutThere--
			req.Err <- err

			consumer.options.LogError("Error finalizing job %d: %s", req.Job.ID, err)
			if err != nil && err != ErrNotFound {
				return err
			}

		// Pause or unpause this connection.
		case consumer.isPaused = <-consumer.pause:
			if consumer.isPaused && job != nil {
				if err = client.Release(job, job.Priority, 0); err != nil {
					consumer.options.LogError("Unable to release job %d: %s", job.ID, err)
				}

				job = nil
				if err != ErrNotFound {
					return err
				}
			}

		// Stop this connection and close this consumer down.
		case <-consumer.stop:
			return nil
		}
	}
}
