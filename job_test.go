package beanstalk

import (
	"testing"
	"time"
)

type TestConsumer struct{}

func (tc *TestConsumer) FinalizeJob(job *Job, method JobMethod, priority uint32, delay time.Duration) error {
	return nil
}

func NewTestJob() *Job {
	return &Job{
		ID:      12345,
		Body:    []byte("Hello World"),
		TTR:     time.Duration(1),
		Manager: &TestConsumer{}}
}

func TestBuryJob(t *testing.T) {
	job := NewTestJob()
	if err := job.Bury(); err != nil {
		t.Fatalf("Unexpected error from Bury: %s", err)
	}
}

func TestBuryJobWithPriority(t *testing.T) {
	job := NewTestJob()
	if err := job.BuryWithPriority(1024); err != nil {
		t.Fatalf("Unexpected error from Bury: %s", err)
	}
}

func TestDeleteJob(t *testing.T) {
	job := NewTestJob()
	if err := job.Delete(); err != nil {
		t.Fatalf("Unexpected error from Delete: %s", err)
	}
}

func TestReleaseJob(t *testing.T) {
	job := NewTestJob()
	if err := job.Release(); err != nil {
		t.Fatalf("Unexpected error from Release: %s", err)
	}
}

func TestReleaseJobWithParams(t *testing.T) {
	job := NewTestJob()
	if err := job.ReleaseWithParams(1024, time.Duration(time.Second)); err != nil {
		t.Fatalf("Unexpected error from Release: %s", err)
	}
}

func TestDoubleFinalizeJob(t *testing.T) {
	job := NewTestJob()
	if err := job.Delete(); err != nil {
		t.Fatalf("Unexpected error from Delete: %s", err)
	}
	if err := job.Delete(); err != ErrJobFinalized {
		t.Fatalf("Expected ErrJobFinalized, but got: %s", err)
	}

}
