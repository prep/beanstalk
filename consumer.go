package beanstalk

import "time"

// Consumer reserves jobs from a beanstalk server and keeps those jobs alive
// until an external consumer has either buried, deleted or released it.
type Consumer struct {
	tubes             []string
	jobC              chan<- *Job
	finalizeJob       chan *finalizeJob
	pause             chan bool
	pauseReservations chan bool
	stop              chan struct{}
	stopJobManager    chan struct{}
}

// NewConsumer returns a new Consumer object.
func NewConsumer(socket string, tubes []string, jobC chan<- *Job, options Options) *Consumer {
	consumer := &Consumer{
		tubes:             tubes,
		jobC:              jobC,
		finalizeJob:       make(chan *finalizeJob),
		pause:             make(chan bool, 1),
		pauseReservations: make(chan bool),
		stop:              make(chan struct{}, 1),
		stopJobManager:    make(chan struct{}),
	}

	go consumer.controlManager()
	go consumer.jobManager(socket, SanitizeOptions(options))

	return consumer
}

// Play allows this consumer to start reserving jobs.
func (consumer *Consumer) Play() {
	consumer.pause <- false
}

// Pause stops this consumer from reserving jobs.
func (consumer *Consumer) Pause() {
	consumer.pause <- true
}

// Stop this consumer running.
func (consumer *Consumer) Stop() {
	consumer.stop <- struct{}{}
}

// controlManager deals with the state changes issued from the Play(), Pause()
// and Stop() functions.
func (consumer *Consumer) controlManager() {
	var paused bool
	var pauseReservations chan bool
	var stopJobManager chan struct{}

	for {
		select {
		case paused = <-consumer.pause:
			pauseReservations = consumer.pauseReservations
		case <-consumer.stop:
			stopJobManager = consumer.stopJobManager

		case pauseReservations <- paused:
			pauseReservations = nil
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
func (consumer *Consumer) jobManager(socket string, options Options) {
	var job *Job
	var jobC chan<- *Job
	var paused, isConnected = true, false

	client := NewClient(socket, options)
	defer client.Close()

	// This timer is used to keep a reserved job alive.
	touchTimer := time.NewTimer(time.Second)
	touchTimer.Stop()

	// reserve is used to trigger a new reserve request in the select statement.
	reserve := make(chan struct{}, 1)

	// reserveJob queues a request for a new job reservation.
	reserveJob := func() {
		if isConnected && !paused && job == nil && len(reserve) == 0 {
			reserve <- struct{}{}
		}
	}

	// releaseJob returns a job back to beanstalk.
	releaseJob := func() {
		if isConnected && job != nil {
			client.Release(job, job.Priority, 0)
			job, jobC = nil, nil
			touchTimer.Stop()
		}
	}

	for {
		select {
		// Reserve a new job.
		case <-reserve:
			if job, _ = client.Reserve(); job != nil {
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
			if err := client.Touch(job); err != nil {
				job, jobC = nil, nil
				break
			}

			touchTimer.Reset(job.TTR)

		// Bury, delete or release the reserved job.
		case req := <-consumer.finalizeJob:
			if req.job != job {
				// This can happen if a disconnect occured before a job was finalized.
				req.ret <- ErrNotFound
				break
			}

			switch req.method {
			case BuryJob:
				req.ret <- client.Bury(req.job, req.priority)
			case DeleteJob:
				req.ret <- client.Delete(req.job)
			case ReleaseJob:
				req.ret <- client.Release(req.job, req.priority, req.delay)
			}

			job = nil
			reserveJob()
			touchTimer.Stop()

		case isConnected = <-client.Connected:
			if !isConnected {
				// The connection was closed, so any reserved jobs are now useless.
				isConnected, job, jobC = false, nil, nil
				touchTimer.Stop()
				break
			}

			for _, tube := range consumer.tubes {
				client.Watch(tube)
			}

			// Ignore the 'default' tube if it wasn't in the list of tubes to watch.
			if !includesString(consumer.tubes, "default") {
				client.Ignore("default")
			}

			reserveJob()

		// Pause or unpause the reservation of new jobs.
		case paused = <-consumer.pauseReservations:
			if paused && jobC != nil {
				// If this job wasn't offered yet, quickly release it.
				releaseJob()
			} else if !paused {
				// If Play() was called, issue a new reservation request.
				reserveJob()
			}

		// Stop this goroutine, but first: release the reserved job, if present.
		case <-consumer.stopJobManager:
			releaseJob()
			return
		}
	}
}
