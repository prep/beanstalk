package beanstalk

import (
	"errors"
	"time"
)

// ErrJobFinalized can get returned on any of the Job's public functions, when
// this Job was already finalized by a previous call.
var ErrJobFinalized = errors.New("Job was already finalized")

// JobMethod describes a type of beanstalk job finalizer.
type JobMethod int

// These are the caller ids that are used when calling back to the Consumer.
const (
	BuryJob JobMethod = iota
	DeleteJob
	ReleaseJob
)

// JobFinalizer defines an interface which a Job can call to be finalized.
type JobFinalizer interface {
	FinalizeJob(*Job, JobMethod, uint32, time.Duration) error
}

// Job contains the data of a reserved job.
type Job struct {
	ID       uint64
	Body     []byte
	Priority uint32
	TTR      time.Duration
	Manager  JobFinalizer
}

func (job *Job) finalizeJob(method JobMethod, priority uint32, delay time.Duration) error {
	if job.Manager == nil {
		return ErrJobFinalized
	}

	ret := job.Manager.FinalizeJob(job, method, priority, delay)
	job.Manager = nil

	return ret
}

// Bury tells the consumer to bury this job with the same priority as this job
// was inserted with.
func (job *Job) Bury() error {
	return job.finalizeJob(BuryJob, job.Priority, 0)
}

// BuryWithPriority tells the consumer to bury this job with the specified
// priority.
func (job *Job) BuryWithPriority(priority uint32) error {
	return job.finalizeJob(BuryJob, priority, 0)
}

// Delete tells the consumer to delete this job.
func (job *Job) Delete() error {
	return job.finalizeJob(DeleteJob, 0, 0)
}

// Release tells the consumer to release this job with the same priority as
// this job was inserted with and without delay.
func (job *Job) Release() error {
	return job.finalizeJob(ReleaseJob, job.Priority, 0)
}

// ReleaseWithParams tells the consumer to release this job with the specified
// priority and delay.
func (job *Job) ReleaseWithParams(priority uint32, delay time.Duration) error {
	return job.finalizeJob(ReleaseJob, priority, delay)
}
