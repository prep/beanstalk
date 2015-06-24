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
	isStopped bool
	jobC      chan<- *Job
	pause     chan bool
	stop      chan struct{}
	sync.Mutex
}

// NewConsumer returns a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options *Options) *Consumer {
	if options == nil {
		options = DefaultOptions()
	}

	consumer := &Consumer{
		tubes: tubes,
		jobC:  jobC,
		pause: make(chan bool, 1),
		stop:  make(chan struct{}, 1),
	}

	go consumer.manager(socket, options)
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

	if len(consumer.pause) != 0 {
		<-consumer.pause
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

	if len(consumer.pause) != 0 {
		<-consumer.pause
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

	consumer.stop <- struct{}{}
	consumer.isStopped = true
	return true
}

// manager takes care of reserving, touching and bury/delete/release-ing of
// beanstalk jobs.
func (consumer *Consumer) manager(socket string, options *Options) {
	var err error
	var client *Client
	var isPaused = true
	var job *Job
	var jobOffer chan<- *Job

	// The queue to keep the reserved jobs in.
	queue := NewJobQueue(options.ReserveWindow)
	var queueItem *JobQueueItem

	// This is used to pause the select-statement for a bit when the job queue
	// is full or when "reserve-with-timeout 0" yields no job.
	timeout := time.NewTimer(time.Second)
	timeout.Stop()

	// The channel to receive requests for finishing jobs on.
	finishJob := make(chan *JobCommand)

	// Start a new connection.
	newConnection, abortConnect := Connect(socket, options)

	// Close the client and reconnect.
	reconnect := func(format string, a ...interface{}) {
		options.LogError(format, a...)

		if client != nil {
			client.Close()
			client = nil

			queue.Clear()

			options.LogInfo("Consumer connection closed. Reconnecting")
			newConnection, abortConnect = Connect(socket, options)
		}
	}

	for {
		job, jobOffer = nil, nil

		if !isPaused && client != nil {
			if queue.IsFull() {
				timeout.Reset(time.Second)
			} else {
				if queue.IsEmpty() {
					job, err = client.Reserve(options.ReserveTimeout)
				} else {
					job, err = client.Reserve(0)
				}

				if err != nil {
					reconnect("Unable to reserve job: %s", err)
				} else if job != nil {
					job.Finish = finishJob
					queue.AddJob(job)
					timeout.Reset(0)
				} else if !queue.IsEmpty() {
					// No job, but still 1 or more jobs from previous attempts left.
					timeout.Reset(time.Second)
				} else {
					// No job, and no reserved jobs from previous attempts.
					timeout.Reset(0)
				}
			}

			if queueItem = queue.ItemForOffer(); queueItem != nil {
				job, jobOffer = queueItem.job, consumer.jobC
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
			queueItem.SetOffered()

		case <-queue.TouchC:
			for _, item := range queue.ItemsForTouch() {
				if err = client.Touch(item.job); err != nil {
					reconnect("%d: Unable to touch job: %s", item.job.ID, err)
					break
				}

				item.Refresh()
			}

			queue.UpdateTimer()

		// Wait a bit before trying to reserve a job again, or just fall through.
		case <-timeout.C:

		// Bury, delete or release a reserved job.
		case finish := <-finishJob:
			if !queue.HasJob(finish.Job) {
				finish.Err <- ErrNotFound
				break
			}

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

			queue.DelJob(finish.Job)
			finish.Err <- err

		// Pause or unpause the action of reserving new jobs.
		case isPaused = <-consumer.pause:
			// When paused, release unoffered jobs in the queue.
			if isPaused {
				for _, job = range queue.UnofferedJobs() {
					client.Release(job, job.Priority, 0)
					queue.DelJob(job)
				}
			}

		// Stop this consumer from running.
		case <-consumer.stop:
			// If a client connection is up, release all jobs in the queue.
			if client != nil {
				for _, job = range queue.Jobs() {
					client.Release(job, job.Priority, 0)
				}

				queue.Clear()
				client.Close()
			}

			if abortConnect != nil {
				abortConnect <- struct{}{}
			}

			return
		}
	}
}
