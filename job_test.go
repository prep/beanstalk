package beanstalk

import (
	"testing"
	"time"
)

func NewTestJob() *Job {
	commandC := make(chan *JobCommand)
	go func() {
		if req, ok := <-commandC; ok {
			req.Err <- nil
		} else {
			return
		}
	}()

	job := &Job{
		ID:       12345,
		Body:     []byte("Hello World"),
		commandC: commandC,
	}
	job.Stats.TTR = time.Duration(1)
	return job
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
	if err := job.Delete(); err != ErrJobFinished {
		t.Fatalf("Expected ErrJobFinished, but got: %s", err)
	}

}

func TestConnectionLostOnJob(t *testing.T) {
	job := NewTestJob()
	close(job.commandC)

	if err := job.Delete(); err != ErrLostConnection {
		t.Fatalf("Expected ErrLostConnection, but got: %s", err)
	}

}
