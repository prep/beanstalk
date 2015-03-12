package beanstalk

import "time"

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	Client
	tubes           []string
	jobC            chan<- *Job
	finalizeJob     chan *finalizeJob
	pause           chan bool
	pauseJobManager chan bool
	stop            chan struct{}
	stopJobManager  chan struct{}
}

// NewConsumer returns a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options *Options) *Consumer {
	consumer := &Consumer{
		Client:          NewClient(socket, options),
		tubes:           tubes,
		jobC:            jobC,
		finalizeJob:     make(chan *finalizeJob),
		pause:           make(chan bool, 1),
		pauseJobManager: make(chan bool),
		stop:            make(chan struct{}, 1),
		stopJobManager:  make(chan struct{}),
	}

	go consumer.controlManager()
	go consumer.jobManager()

	return consumer
}

// Play makes this consumer reserve jobs.
func (consumer *Consumer) Play() {
	consumer.pause <- false
}

// Pause stops this consumer from reserving jobs.
func (consumer *Consumer) Pause() {
	consumer.pause <- true
}

// Stop this consumer from running.
func (consumer *Consumer) Stop() {
	consumer.stop <- struct{}{}
}

// controlManager deals with the state changes issued from the Play(), Pause()
// and Stop() functions.
func (consumer *Consumer) controlManager() {
	var paused bool
	var pauseJobManager chan bool
	var stopJobManager chan struct{}

	for {
		select {
		case paused = <-consumer.pause:
			pauseJobManager = consumer.pauseJobManager
		case <-consumer.stop:
			stopJobManager = consumer.stopJobManager

		case pauseJobManager <- paused:
			pauseJobManager = nil
		case stopJobManager <- struct{}{}:
			return
		}
	}
}

type finalizeJob struct {
	job      *Job
	method   JobMethod
	ret      chan error
	priority uint32
	delay    time.Duration
}

// FinalizeJob is an interface function for Job that gets called whenever it is
// decided to finalize the job by either burying, deleting or releasing it.
func (consumer *Consumer) FinalizeJob(job *Job, method JobMethod, priority uint32, delay time.Duration) error {
	fJob := &finalizeJob{job: job, method: method, ret: make(chan error), priority: priority, delay: delay}
	consumer.finalizeJob <- fJob
	return <-fJob.ret
}

// jobManager is responsible for reserving beanstalk jobs and keeping those
// jobs reserved until they're either buried, deleted or released.
func (consumer *Consumer) jobManager() {
	var job *Job
	var jobC chan<- *Job
	var paused = true

	consumer.OpenConnection()
	defer consumer.CloseConnection()

	// This timer is used to keep a reserved job alive.
	touchTimer := time.NewTimer(time.Second)
	touchTimer.Stop()

	// reserve is used to trigger a new reserve request in the select statement.
	reserve := make(chan struct{}, 1)

	// reserveJob queues a request for a new job reservation.
	reserveJob := func() {
		if consumer.isConnected && !paused && job == nil && len(reserve) == 0 {
			reserve <- struct{}{}
		}
	}

	// releaseJob returns a job back to beanstalk.
	releaseJob := func() {
		if job != nil {
			consumer.Release(job, job.Priority, 0)
			job, jobC = nil, nil
			touchTimer.Stop()
		}
	}

	for {
		select {
		// Reserve a new job.
		case <-reserve:
			if job, _ = consumer.Reserve(); job != nil {
				jobC, job.Manager = consumer.jobC, consumer
				touchTimer.Reset(job.TTR)
			} else {
				reserveJob()
			}

		// If a job was reserved, offer it up.
		case jobC <- job:
			jobC = nil

		// Touch the reserved job at a regular interval to keep it reserved.
		case <-touchTimer.C:
			if err := consumer.Touch(job); err != nil {
				job, jobC = nil, nil
				break
			}

			touchTimer.Reset(job.TTR)

		// Bury, delete or release the reserved job.
		case req := <-consumer.finalizeJob:
			// This can happen if a disconnect occured before a job was finalized.
			if req.job != job {
				req.ret <- ErrNotFound
				break
			}

			switch req.method {
			case BuryJob:
				req.ret <- consumer.Bury(req.job, req.priority)
			case DeleteJob:
				req.ret <- consumer.Delete(req.job)
			case ReleaseJob:
				req.ret <- consumer.Release(req.job, req.priority, req.delay)
			}

			job = nil
			reserveJob()
			touchTimer.Stop()

		// Set up a new connection.
		case conn := <-consumer.connCreatedC:
			consumer.SetConnection(conn)
			for _, tube := range consumer.tubes {
				consumer.Watch(tube)
			}

			// Ignore the 'default' tube if it wasn't in the list of tubes to watch.
			if !includesString(consumer.tubes, "default") {
				consumer.Ignore("default")
			}

			reserveJob()

		// The connection was closed, so any reserved jobs are now useless.
		case <-consumer.connClosedC:
			job, jobC = nil, nil
			touchTimer.Stop()

		// Pause or unpause reservering new jobs.
		case paused = <-consumer.pauseJobManager:
			// If this job wasn't offered yet, quickly release it.
			if paused && jobC != nil {
				releaseJob()
			} else if !paused {
				reserveJob()
			}

		// Stop this goroutine. Release the job, if one is pending.
		case <-consumer.stopJobManager:
			releaseJob()
			return
		}
	}
}
