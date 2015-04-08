package beanstalk

import (
	"strings"
	"time"
)

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	tubes []string
	jobC  chan<- *Job
	pause chan bool
	stop  chan struct{}
}

// NewConsumer returns a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options Options) *Consumer {
	consumer := &Consumer{
		tubes: tubes,
		jobC:  jobC,
		pause: make(chan bool, 1),
		stop:  make(chan struct{}, 1),
	}

	go consumer.manager(socket, SanitizeOptions(options))
	return consumer
}

// Play allows this consumer to start reserving jobs.
func (consumer *Consumer) Play() {
	select {
	case <-consumer.pause:
	default:
	}

	consumer.pause <- false
}

// Pause stops this consumer from reserving jobs.
func (consumer *Consumer) Pause() {
	select {
	case <-consumer.pause:
	default:
	}

	consumer.pause <- true
}

// Stop this consumer.
func (consumer *Consumer) Stop() {
	consumer.stop <- struct{}{}
}

// manager takes care of reserving, touching and bury/delete/release-ing of
// beanstalk jobs.
func (consumer *Consumer) manager(socket string, options Options) {
	var client *Client
	var job *Job
	var jobOffer chan<- *Job
	var err error
	var isPaused = true

	// Set up a new connection.
	newConnection, abortConnect := Connect(socket, options)

	// Close the client and reconnect.
	reconnect := func(format string, a ...interface{}) {
		options.LogError(format, a...)

		if client != nil {
			client.Close()
			client, job, jobOffer = nil, nil, nil
			options.LogInfo("Consumer connection closed. Reconnecting")
			newConnection, abortConnect = Connect(socket, options)
		}
	}

	// This timer is used to keep a reserved job alive.
	touchTimer := time.NewTimer(time.Second)
	touchTimer.Stop()

	// releaseJob returns a job back to beanstalk.
	releaseJob := func() {
		if client != nil && job != nil {
			client.Release(job, job.Priority, 0)
			job, jobOffer = nil, nil
			touchTimer.Stop()
		}
	}

	// The channel to receive requests for finishing jobs on.
	finishJob := make(chan *JobCommand)

	for {
		// Reserve a new job, if the state allows for it.
		if !isPaused && client != nil && job == nil {
			select {
			// A job wanting to finish at this stage is never successful.
			case finish := <-finishJob:
				finish.Err <- ErrNotFound
				continue

			case isPaused = <-consumer.pause:
				continue

			case <-consumer.stop:
				client.Close()
				return

			default:
			}

			// Try to reserve a new job.
			if job, err = client.Reserve(); err != nil {
				reconnect("Unable to reserve job: %s", err)
			} else if job != nil {
				jobOffer, job.Finish = consumer.jobC, finishJob
				touchTimer.Reset(job.TTR)
			} else {
				continue
			}
		}

		select {
		// Set up a new beanstalk client connection and watch the tubes.
		case conn := <-newConnection:
			client, abortConnect = NewClient(conn, options), nil

			options.LogInfo("Watching tubes: %s", strings.Join(consumer.tubes, ", "))
			for _, tube := range consumer.tubes {
				if err = client.Watch(tube); err != nil {
					reconnect("Error watching tube: %s", err)
					break
				}
			}

			if err == nil && !includesString(consumer.tubes, "default") {
				if err = client.Ignore("default"); err != nil {
					reconnect("Unable to ignore tube: %s", err)
				}
			}

		// Offer the job up on the shared jobs channel.
		case jobOffer <- job:
			jobOffer = nil

		// Regularly touch a job to keep it reserved.
		case <-touchTimer.C:
			if err = client.Touch(job); err != nil {
				reconnect("Unable to touch job: %s", err)
				job, jobOffer = nil, nil
				break
			}

			touchTimer.Reset(job.TTR)

		// Bury, delete or release a reserved job.
		case finish := <-finishJob:
			if job != finish.Job {
				finish.Err <- ErrNotFound
				break
			}

			touchTimer.Stop()

			switch finish.Command {
			case Bury:
				err = client.Bury(finish.Job, finish.Priority)
			case Delete:
				err = client.Delete(finish.Job)
			case Release:
				err = client.Release(finish.Job, finish.Priority, finish.Delay)
			}

			if err != nil {
				reconnect("Unable to finish job: %s", err)
			}

			job = nil
			finish.Err <- err

		// Pause or unpause the action of reserving new jobs.
		case isPaused = <-consumer.pause:
			if isPaused && jobOffer != nil {
				releaseJob()
			}

		// Stop this consumer from running.
		case <-consumer.stop:
			releaseJob()

			if client != nil {
				client.Close()
			}

			if abortConnect != nil {
				abortConnect <- struct{}{}
			}

			return
		}
	}
}
