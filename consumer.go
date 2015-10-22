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
	resize    chan int
	isPaused  bool
	isStopped bool
	options   *Options
	client    *Client
	queue     *JobQueue
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
		resize:   make(chan int, 1),
		isPaused: true,
		options:  options,
		queue:    NewJobQueue(options.QueueSize),
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

	consumer.stop <- struct{}{}
	consumer.isStopped = true
	return true
}

// ResizeQueue resizes the number of jobs can be reserved in a queue.
func (consumer *Consumer) ResizeQueue(size int) bool {
	consumer.Lock()
	defer consumer.Unlock()

	if consumer.isStopped {
		return false
	}

	if len(consumer.resize) != 0 {
		<-consumer.resize
	}

	consumer.resize <- size
	return true
}

// isLive returns true when the connection is established and unpaused.
func (consumer *Consumer) isLive() bool {
	return (!consumer.isPaused && consumer.client != nil)
}

// reserveJob attempts to reserve a beanstalk job. It tries to be smart about
// when to reserve with a timeout and when to check with immediate return.
func (consumer *Consumer) reserveJob() (*Job, error) {
	if !consumer.isLive() {
		return nil, nil
	}

	var job *Job
	var err error

	if consumer.queue.IsEmpty() {
		job, err = consumer.client.Reserve(consumer.options.ReserveTimeout)
	} else {
		job, err = consumer.client.Reserve(0)
	}
	if err != nil {
		return nil, err
	}

	return job, nil
}

// manager takes care of reserving, touching and bury/delete/release-ing of
// beanstalk jobs.
func (consumer *Consumer) manager(socket string, options *Options) {
	var err error
	var job *Job
	var jobOffer chan<- *Job
	var queueItem *JobQueueItem
	var finishJob = make(chan *JobCommand)

	// This is used to pause the select-statement for a bit when the job queue
	// is full or when "reserve-with-timeout 0" yields no job.
	timeout := time.NewTimer(time.Second)
	timeout.Stop()

	// Start a new connection.
	newConnection, abortConnect := Connect(socket, options)

	// Close the current connection, clear the jobs queue and try to reconnect.
	reconnect := func(format string, a ...interface{}) {
		options.LogError(format, a...)

		if consumer.client != nil {
			consumer.client.Close()
			consumer.client = nil

			consumer.queue.Clear()

			options.LogInfo("Consumer connection closed. Reconnecting")
			newConnection, abortConnect = Connect(socket, options)
		}
	}

	for {
		job, jobOffer = nil, nil

		if consumer.isLive() {
			// Try to reserve a job if the job queue isn't full already.
			if !consumer.queue.IsFull() {
				if job, err = consumer.reserveJob(); err != nil && err != ErrDeadlineSoon {
					reconnect("Unable to reserve job: %s", err)
				} else if job != nil {
					timeout.Reset(0)
					job.Finish = finishJob
					consumer.queue.AddJob(job)
				} else if !consumer.queue.IsEmpty() {
					timeout.Reset(time.Second)
				} else {
					timeout.Reset(0)
				}
			} else {
				timeout.Reset(time.Second)
			}

			// Fetch the oldest job in the queue to offer up.
			if queueItem = consumer.queue.ItemForOffer(); queueItem != nil {
				job, jobOffer = queueItem.job, consumer.jobC
			}
		}

		select {
		// Set up a new beanstalk client connection and watch the tubes.
		case conn := <-newConnection:
			consumer.client, abortConnect = NewClient(conn, options), nil

			options.LogInfo("Watching tubes: %s", strings.Join(consumer.tubes, ", "))
			for _, tube := range consumer.tubes {
				if err = consumer.client.Watch(tube); err != nil {
					reconnect("Error watching tube: %s", err)
					break
				}
			}

			if err == nil && !includesString(consumer.tubes, "default") {
				if err = consumer.client.Ignore("default"); err != nil {
					reconnect("Unable to ignore tube: %s", err)
				}
			}

		// Offer the job up on the shared jobs channel.
		case jobOffer <- job:
			queueItem.SetOffered()

		// Touch the jobs that need touching.
		case <-consumer.queue.TouchC:
			for _, item := range consumer.queue.ItemsForTouch() {
				if err = consumer.client.Touch(item.job); err != nil {
					reconnect("Unable to touch job %d: %s", item.job.ID, err)
					break
				}

				item.Refresh()
			}

			consumer.queue.UpdateTimer()

		// Wait a bit before trying to reserve a job again, or just fall through.
		case <-timeout.C:

		// Bury, delete or release a reserved job.
		case finish := <-finishJob:
			if !consumer.queue.HasJob(finish.Job) {
				finish.Err <- ErrNotFound
				break
			}

			switch finish.Command {
			case Bury:
				err = consumer.client.Bury(finish.Job, finish.Priority)
			case Delete:
				err = consumer.client.Delete(finish.Job)
			case Release:
				err = consumer.client.Release(finish.Job, finish.Priority, finish.Delay)
			}

			if err != nil {
				reconnect("Unable to finish job: %s", err)
			}

			consumer.queue.DelJob(finish.Job)
			finish.Err <- err

		// Resize the jobs queue.
		case size := <-consumer.resize:
			consumer.queue.Resize(size)

		// Pause or unpause the action of reserving new jobs.
		case consumer.isPaused = <-consumer.pause:
			// When paused, release unoffered jobs in the queue.
			if consumer.isPaused {
				for _, job = range consumer.queue.UnofferedJobs() {
					consumer.client.Release(job, job.Priority, 0)
					consumer.queue.DelJob(job)
				}
			}

		// Stop this consumer from running.
		case <-consumer.stop:
			// If a client connection is up, release all jobs in the queue.
			if consumer.client != nil {
				for _, job = range consumer.queue.Jobs() {
					consumer.client.Release(job, job.Priority, 0)
				}

				consumer.queue.Clear()
				consumer.client.Close()
			}

			if abortConnect != nil {
				abortConnect <- struct{}{}
			}

			return
		}
	}
}
