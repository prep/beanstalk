package beanstalk

import (
	"errors"
	"sync"
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
	Touch
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
	ID        uint64
	Body      []byte
	Priority  uint32
	TTR       time.Duration
	touchedAt time.Time
	commandC  chan<- *JobCommand
	sync.Mutex
}

func (job *Job) cmd(command Command, priority uint32, delay time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrLostConnection
		}
	}()

	if job.commandC == nil {
		return ErrJobFinished
	}

	jobCommand := &JobCommand{
		Command:  command,
		Job:      job,
		Priority: priority,
		Delay:    delay,
		Err:      make(chan error),
	}

	job.commandC <- jobCommand
	if command != Touch {
		job.commandC = nil
	}

	return <-jobCommand.Err
}

// Bury tells the consumer to bury this job with the same priority as this job
// was inserted with.
func (job *Job) Bury() error {
	return job.cmd(Bury, job.Priority, 0)
}

// BuryWithPriority tells the consumer to bury this job with the specified
// priority.
func (job *Job) BuryWithPriority(priority uint32) error {
	return job.cmd(Bury, priority, 0)
}

// Delete tells the consumer to delete this job.
func (job *Job) Delete() error {
	return job.cmd(Delete, 0, 0)
}

// Release tells the consumer to release this job with the same priority as
// this job was inserted with and without delay.
func (job *Job) Release() error {
	return job.cmd(Release, job.Priority, 0)
}

// ReleaseWithParams tells the consumer to release this job with the specified
// priority and delay.
func (job *Job) ReleaseWithParams(priority uint32, delay time.Duration) error {
	return job.cmd(Release, priority, delay)
}

// Touch refreshes the reserve timer for this job.
func (job *Job) Touch() error {
	return job.cmd(Touch, 0, 0)
}

// Mark the time this job was touched.
func (job *Job) touched() {
	job.Lock()
	job.touchedAt = time.Now().UTC()
	job.Unlock()
}

// TouchAt returns the duration after which a call to Touch() should be
// triggered in order to keep the job reserved.
func (job *Job) TouchAt() time.Duration {
	var margin time.Duration

	switch {
	case job.TTR <= 3*time.Second:
		margin = 200 * time.Millisecond
	case job.TTR < 60*time.Second:
		margin = time.Second
	default:
		margin = 3 * time.Second
	}

	job.Lock()
	defer job.Unlock()

	return job.touchedAt.Add(job.TTR - margin).Sub(time.Now().UTC())
}
