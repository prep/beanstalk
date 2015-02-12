package beanstalk

import "time"

// JobMethod describes a type of beanstalk job finalizer.
type JobMethod int

// These are the caller ids that are used when calling back to the Consumer.
const (
	BuryJob JobMethod = iota
	DeleteJob
	ReleaseJob
)

// JobFinisher defines an interface which a Job can call to finish up.
type JobFinisher interface {
	FinishJob(*Job, JobMethod, uint32, int) error
}

// Job contains the data of a reserved job.
type Job struct {
	ID     uint64
	Body   []byte
	TTR    time.Duration
	Finish JobFinisher
}

func (job *Job) finishJob(method JobMethod, priority uint32, delay int) error {
	ret := job.Finish.FinishJob(job, method, priority, delay)
	job.Finish = nil
	return ret
}

// Bury tells the consumer to bury this job.
func (job *Job) Bury(priority uint32) error {
	return job.finishJob(BuryJob, priority, 0)
}

// Delete tells the consumer to delete this job.
func (job *Job) Delete() error {
	return job.finishJob(DeleteJob, 0, 0)
}

// Release tells the consumer to release this job.
func (job *Job) Release(priority uint32, delay int) error {
	return job.finishJob(ReleaseJob, priority, delay)
}
