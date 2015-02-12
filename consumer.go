package beanstalk

import (
	"errors"
	"time"
)

// ErrJobLost can be returned by FinishJob() when a Job wants to finalize,
// but the connection was lost in the meantime, or the job reservation
// couldn't be kept.
var ErrJobLost = errors.New("Job was lost")

type finishJobRequest struct {
	method   JobMethod
	ret      chan error
	priority uint32
	delay    time.Duration
}

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	Client
	tubes     []string
	jobC      chan *Job
	finishJob chan *finishJobRequest
	pause     chan bool
	stop      chan struct{}
}

// NewConsumer creates a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan *Job, options *Options) *Consumer {
	consumer := &Consumer{
		Client:    NewClient(socket, options),
		tubes:     tubes,
		jobC:      jobC,
		finishJob: make(chan *finishJobRequest),
		pause:     make(chan bool),
		stop:      make(chan struct{}, 1)}

	go consumer.jobManager()
	return consumer
}

// Stop signals the connnectionManager() goroutine to close its connection and
// stop running.
func (consumer *Consumer) Stop() {
	consumer.stop <- struct{}{}
}

// Play signals this consumer to start reserving jobs.
func (consumer *Consumer) Play() {
	consumer.pause <- false
}

// Pause signals this consumer to stop reserving jobs.
func (consumer *Consumer) Pause() {
	consumer.pause <- true
}

// FinishJob is an interface function for Job that gets called whenever it is
// decided to finalize the job by either burying, deleting or releasing it.
func (consumer *Consumer) FinishJob(job *Job, method JobMethod, priority uint32, delay time.Duration) error {
	req := &finishJobRequest{method: method, ret: make(chan error), priority: priority, delay: delay}
	consumer.finishJob <- req
	return <-req.ret
}

// jobManager is responsible for maintaining a connection to the beanstalk
// server, reserving jobs, keeping them reserved and finalizing them.
func (consumer *Consumer) jobManager() {
	var job *Job
	var jobC chan *Job
	var paused, offered = true, false

	consumer.OpenConnection()
	defer consumer.CloseConnection()

	// This timer is used to keep a reserved job alive.
	touchTimer := time.NewTimer(time.Second)
	touchTimer.Stop()

	// If a connection is up but no job could be reserved, use a fallthrough
	// channel to make the select statement non-blocking.
	fallThrough := newFallThrough()

	for {
		fallThrough.Clear()

		if !paused && consumer.isConnected && job == nil {
			if job, _ = consumer.Reserve(); job != nil {
				job.Finish, jobC, offered = consumer, consumer.jobC, false
				touchTimer.Reset(job.TTR)
			} else {
				fallThrough.Set()
			}
		}

		select {
		// Offer a reserved job up and nullify jobC on success, to make sure the
		// job doesn't get sent twice.
		case jobC <- job:
			jobC, offered = nil, true

		// Regularly touch the reserved job to keep it reserved.
		case <-touchTimer.C:
			if err := consumer.Touch(job); err != nil {
				job = nil
				break
			}

			touchTimer.Reset(job.TTR)

		// Finalize a job, which means either bury, delete or release it.
		case req := <-consumer.finishJob:
			if job == nil {
				req.ret <- ErrJobLost
			}

			switch req.method {
			case BuryJob:
				req.ret <- consumer.Bury(job, req.priority)
			case DeleteJob:
				req.ret <- consumer.Delete(job)
			case ReleaseJob:
				req.ret <- consumer.Release(job, req.priority, req.delay)
			}

			job = nil
			touchTimer.Stop()

		// Set up a new connection and watch the relevant tubes.
		case conn := <-consumer.connCreatedC:
			consumer.SetConnection(conn)

			var err error
			for _, tube := range consumer.tubes {
				if err = consumer.Watch(tube); err != nil {
					break
				}
			}

			// Ignore the 'default' tube if it wasn't in the list of tubes to watch.
			if err == nil && includesString(consumer.tubes, "default") {
				consumer.Ignore("default")
			}

		// The connection was closed, so any reserved jobs are now useless.
		case <-consumer.connClosedC:
			touchTimer.Stop()
			job, jobC = nil, nil

		// Fallthrough in case no job could be reserved, but a quick channel check
		// is needed before trying another reserve request.
		case <-fallThrough.C:

		//
		case paused = <-consumer.pause:
			if job != nil && !offered {
				job.
			}

		// Stop this goroutine from running.
		case <-consumer.stop:
			return
		}
	}
}
