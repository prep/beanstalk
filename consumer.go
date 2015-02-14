package beanstalk

import "time"

type finalizeJob struct {
	job      *Job
	method   JobMethod
	ret      chan error
	priority uint32
	delay    time.Duration
}

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	Client
	tubes       []string
	jobC        chan<- *Job
	finalizeJob chan *finalizeJob
	reserve     chan struct{}
	reservedJob chan *Job
	pause       chan bool
	stop        chan struct{}
}

// NewConsumer creates a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options *Options) *Consumer {
	consumer := &Consumer{
		Client:      NewClient(socket, options),
		tubes:       tubes,
		jobC:        jobC,
		finalizeJob: make(chan *finalizeJob),
		reserve:     make(chan struct{}),
		reservedJob: make(chan *Job),
		pause:       make(chan bool, 1),
		stop:        make(chan struct{}, 1)}

	go consumer.jobReserver()
	go consumer.jobManager()

	return consumer
}

// Stop tells the jobManager() goroutine to stop running.
func (consumer *Consumer) Stop() {
	consumer.stop <- struct{}{}
}

// Play makes this consumer reserve jobs.
func (consumer *Consumer) Play() {
	consumer.pause <- false
}

// Pause stops this consumer from reserving jobs.
func (consumer *Consumer) Pause() {
	consumer.pause <- true
}

// FinalizeJob is an interface function for Job that gets called whenever it is
// decided to finalize the job by either burying, deleting or releasing it.
func (consumer *Consumer) FinalizeJob(job *Job, method JobMethod, priority uint32, delay time.Duration) error {
	fJob := &finalizeJob{job: job, method: method, ret: make(chan error), priority: priority, delay: delay}
	consumer.finalizeJob <- fJob
	return <-fJob.ret
}

// jobReserver simply reserves jobs.
func (consumer *Consumer) jobReserver() {
	for {
		select {
		case _, ok := <-consumer.reserve:
			if !ok {
				close(consumer.reservedJob)
				return
			}

			job, _ := consumer.Reserve()
			consumer.reservedJob <- job
		}
	}
}

// jobManager is responsible for maintaining a connection to the beanstalk
// server, reserving jobs, keeping them reserved and finalizing them.
func (consumer *Consumer) jobManager() {
	var job *Job
	var jobC chan<- *Job
	var paused, requested, offered, ok = true, false, false, true

	consumer.OpenConnection()
	defer consumer.CloseConnection()

	// This timer is used to keep a reserved job alive.
	touchTimer := time.NewTimer(time.Second)
	touchTimer.Stop()

	// reserveJob fetches a new job if the state allows for it.
	reserveJob := func() {
		if !requested && !paused && consumer.isConnected && job == nil {
			consumer.reserve <- struct{}{}
			requested = true
		}
	}

	// releaseJob releases a job back to beanstalk when it hasn't already been
	// offered up.
	releaseJob := func() {
		if job != nil && !offered {
			consumer.Release(job, job.Priority, 0)
			job, jobC = nil, nil
			touchTimer.Stop()
		}
	}

	for {
		select {
		// Wait for a new reserved job.
		case job, ok = <-consumer.reservedJob:
			// If this channel closes, exit this goroutine.
			if !ok {
				return
			}

			requested, offered = false, false

			// If no job could be reserved, try again.
			if job == nil {
				reserveJob()
				break
			}

			// If this consumer was paused in the meantime, release the job.
			if paused {
				releaseJob()
				break
			}

			jobC, job.Manager = consumer.jobC, consumer
			touchTimer.Reset(job.TTR)

		// Offer up the reserved job.
		case jobC <- job:
			jobC, offered = nil, true

		// Keep the job reserved by regularly touching it.
		case <-touchTimer.C:
			if err := consumer.Touch(job); err != nil {
				job, jobC = nil, nil
				break
			}

			touchTimer.Reset(job.TTR)

		// Finalize a job, which means either bury, delete or release it.
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

		// Play or pause this consumer.
		case paused = <-consumer.pause:
			if paused {
				releaseJob()
			} else {
				reserveJob()
			}

		// Closing the reserve channel tells jobReserver() to stop running.
		case <-consumer.stop:
			releaseJob()
			paused = true
			close(consumer.reserve)
		}
	}
}
