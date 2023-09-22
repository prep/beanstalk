package beanstalk

import (
	"context"
	"errors"
	"time"
)

// ErrJobFinished is returned when a job was already finished.
var ErrJobFinished = errors.New("job was already finished")

// PutParams are the parameters used to perform a Put operation.
type PutParams struct {
	Priority uint32        `yaml:"pri"`
	Delay    time.Duration `yaml:"delay"`
	TTR      time.Duration `yaml:"ttr"`
}

// ReleaseFunc describes the function that is called whenever a job is buried,
// deleted, or released.
type ReleaseFunc func(tube string)

// Job describes a beanstalk job and its stats.
type Job struct {
	ID         uint64
	Body       []byte
	ReservedAt time.Time
	Stats      struct {
		PutParams `yaml:",inline"`
		Tube      string        `yaml:"tube"`
		State     string        `yaml:"state"`
		Age       time.Duration `yaml:"age"`
		TimeLeft  time.Duration `yaml:"time-left"`
		File      int           `yaml:"file"`
		Reserves  int           `yaml:"reserves"`
		Timeouts  int           `yaml:"timeouts"`
		Releases  int           `yaml:"releases"`
		Buries    int           `yaml:"buries"`
		Kicks     int           `yaml:"kicks"`
	}
	ReleaseFunc ReleaseFunc
	conn        *Conn
}

// Bury this job.
func (job *Job) Bury(ctx context.Context) error {
	return job.BuryWithPriority(ctx, job.Stats.Priority)
}

// BuryWithPriority buries this job with the specified priority.
func (job *Job) BuryWithPriority(ctx context.Context, priority uint32) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	err := job.conn.Bury(ctx, job, priority)
	job.conn = nil
	return job.handleRelease(err)
}

// Delete this job.
func (job *Job) Delete(ctx context.Context) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	err := job.conn.Delete(ctx, job.ID)
	job.conn = nil
	return job.handleRelease(err)
}

// Release this job back with its original priority and without delay.
func (job *Job) Release(ctx context.Context) error {
	return job.ReleaseWithParams(ctx, job.Stats.Priority, 0)
}

// ReleaseWithParams releases this job back with the specified priority and delay.
func (job *Job) ReleaseWithParams(ctx context.Context, priority uint32, delay time.Duration) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	err := job.conn.Release(ctx, job, priority, delay)
	job.conn = nil
	return job.handleRelease(err)
}

func (job *Job) handleRelease(err error) error {
	if err != nil {
		return err
	}
	if job.ReleaseFunc != nil {
		job.ReleaseFunc(job.Stats.Tube)
	}

	return nil
}

// Touch the job thereby resetting its reserved status.
func (job *Job) Touch(ctx context.Context) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	return job.conn.Touch(ctx, job)
}

// TouchAfter returns the duration until this jobs needs to be touched for its
// reservation to be retained.
func (job *Job) TouchAfter() time.Duration {
	// ReservedAt is not set for jobs that are peeked.
	if job.ReservedAt.IsZero() {
		return 0
	}

	return time.Until(job.ReservedAt.Add(job.Stats.TimeLeft))
}

// Kick moves the job into the ready queue.
func (job *Job) Kick(ctx context.Context) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	return job.conn.KickJob(ctx, job)
}
