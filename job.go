package beanstalk

import (
	"errors"
	"time"
)

// The errors that can be returned by any of the Job functions.
var (
	ErrJobFinished    = errors.New("Job was already finished")
	ErrLostConnection = errors.New("The connection was lost")
)

// Command describes a beanstalk command that finishes a reserved job.
type Command int

// These are the caller ids that are used when calling back to the Consumer.
const (
	Bury Command = iota
	Delete
	Release
)

// JobCommand is sent to the consumer that reserved the job with the purpose
// of finishing a job.
type JobCommand struct {
	Command  Command
	Job      *Job
	Priority uint32
	Delay    time.Duration
	Err      chan error
}

// Job contains the data of a reserved job.
type Job struct {
	ID       uint64
	Body     []byte
	Priority uint32
	TTR      time.Duration
	Finish   chan<- *JobCommand
}

func (job *Job) finishJob(command Command, priority uint32, delay time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrLostConnection
		}
	}()

	if job.Finish == nil {
		return ErrJobFinished
	}

	jobCommand := &JobCommand{Command: command, Job: job, Priority: priority, Delay: delay, Err: make(chan error)}
	job.Finish <- jobCommand
	job.Finish = nil
	return <-jobCommand.Err
}

// Bury tells the consumer to bury this job with the same priority as this job
// was inserted with.
func (job *Job) Bury() error {
	return job.finishJob(Bury, job.Priority, 0)
}

// BuryWithPriority tells the consumer to bury this job with the specified
// priority.
func (job *Job) BuryWithPriority(priority uint32) error {
	return job.finishJob(Bury, priority, 0)
}

// Delete tells the consumer to delete this job.
func (job *Job) Delete() error {
	return job.finishJob(Delete, 0, 0)
}

// Release tells the consumer to release this job with the same priority as
// this job was inserted with and without delay.
func (job *Job) Release() error {
	return job.finishJob(Release, job.Priority, 0)
}

// ReleaseWithParams tells the consumer to release this job with the specified
// priority and delay.
func (job *Job) ReleaseWithParams(priority uint32, delay time.Duration) error {
	return job.finishJob(Release, priority, delay)
}

// TouchAfter returns the time after which this job needs to be touched in
// order to remain reserved on the beantalk server.
func (job *Job) TouchAfter() time.Duration {
	switch {
	case job.TTR <= time.Second:
		return time.Duration(800 * time.Millisecond)

	case job.TTR < 60*time.Second:
		return job.TTR - time.Second

	default:
		return job.TTR - (3 * time.Second)
	}
}
