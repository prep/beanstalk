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

// Job describes a beanstalk job and its stats.
type Job struct {
	ID         uint64
	Body       []byte
	ReservedAt time.Time
	Stats      struct {
		PutParams
		Tube     string        `yaml:"tube"`
		State    string        `yaml:"state"`
		Age      time.Duration `yaml:"age"`
		TimeLeft time.Duration `yaml:"time-left"`
		File     int           `yaml:"file"`
		Reserves int           `yaml:"reserves"`
		Timeouts int           `yaml:"timeouts"`
		Releases int           `yaml:"releases"`
		Buries   int           `yaml:"buries"`
		Kicks    int           `yaml:"kicks"`
	}

	conn *Conn
	errC chan error
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

	err := job.conn.bury(ctx, job, priority)
	job.conn = nil
	return err
}

// Delete this job.
func (job *Job) Delete(ctx context.Context) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	err := job.conn.delete(ctx, job)
	job.conn = nil
	return err
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

	err := job.conn.release(ctx, job, priority, delay)
	job.conn = nil
	return err
}

// Touch the job thereby resetting its reserved status.
func (job *Job) Touch(ctx context.Context) error {
	if job.conn == nil {
		return ErrJobFinished
	}

	return job.conn.touch(ctx, job)
}

// TouchAfter returns the duration until this jobs needs to be touched for its
// reservation to be retained.
func (job *Job) TouchAfter() time.Duration {
	expiresAfter := time.Until(job.ReservedAt.Add(job.Stats.TimeLeft))

	switch {
	case expiresAfter < 500*time.Millisecond:
		return 0
	case expiresAfter < 3*time.Second:
		return expiresAfter - 500*time.Millisecond
	case expiresAfter < 60*time.Second:
		return expiresAfter - time.Second
	default:
		return expiresAfter - 3*time.Second
	}
}
