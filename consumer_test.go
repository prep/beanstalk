package beanstalk

import (
	"log"
	"net"
	"net/textproto"
	"os"
	"testing"
	"time"
)

var waitABit = 20 * time.Millisecond

type TestConsumer struct {
	*Consumer
	t           *testing.T
	listener    net.Listener
	client      *textproto.Conn
	offeredJobC chan *Job
	requestC    chan string
	responseC   chan string
}

func NewTestConsumer(t *testing.T) *TestConsumer {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic("Unable to create consumer socket: " + err.Error())
	}

	jobC := make(chan *Job)

	options := &Options{
		ReserveTimeout:   time.Second,
		ReconnectTimeout: time.Second * 3,
		ErrorLog:         log.New(os.Stdout, "ERROR: ", 0),
	}

	consumer := &TestConsumer{
		Consumer:    NewConsumer(listener.Addr().String(), []string{"test"}, jobC, options),
		t:           t,
		listener:    listener,
		offeredJobC: jobC,
		requestC:    make(chan string),
		responseC:   make(chan string),
	}

	go consumer.acceptNewConnections()

	consumer.ExpectWatchAndIgnore()
	return consumer
}

func (consumer *TestConsumer) Close() {
	consumer.Stop()
	consumer.listener.Close()
}

func (consumer *TestConsumer) acceptNewConnections() {
	for {
		conn, err := consumer.listener.Accept()
		if err != nil {
			return
		}

		consumer.client = textproto.NewConn(conn)
		consumer.handleConnection()
	}
}

func (consumer *TestConsumer) handleConnection() {
	defer consumer.client.Close()

	for {
		request, err := consumer.client.ReadLine()
		if err != nil {
			break
		}

		consumer.requestC <- request

		response := <-consumer.responseC
		consumer.client.PrintfLine(response)
	}
}

func (consumer *TestConsumer) Expect(line string) chan string {
	select {
	case request := <-consumer.requestC:
		if request != line {
			consumer.t.Fatalf("Expected '%s', but got '%s' instead", line, request)
		}
		return consumer.responseC

	case <-time.After(waitABit):
		consumer.t.Fatalf("Expected '%s', but received nothing", line)
	}

	return nil
}

func (consumer *TestConsumer) ExpectWatchAndIgnore() {
	consumer.Expect("watch test") <- "WATCHING 2"
	consumer.Expect("ignore default") <- "WATCHING 1"

	if consumer.isPaused {
		consumer.ExpectNoRequest()
	}
}

func (consumer *TestConsumer) ExpectNoRequest() {
	select {
	case request := <-consumer.requestC:
		consumer.t.Fatalf("Didn't expect a command from the client, but got '%s' instead", request)
	case <-time.After(waitABit):
	}
}

func (consumer *TestConsumer) ExpectJobOffer() *Job {
	select {
	case job := <-consumer.offeredJobC:
		return job
	case <-time.After(waitABit):
		consumer.t.Fatalf("Expected a job to be offered, but got nothing")
	}

	return nil
}

func (consumer *TestConsumer) FinalizeJob(job *Job, command string) {
	go func(job *Job, command string) {
		var err error

		switch command {
		case "bury":
			err = job.Bury()
		case "delete":
			err = job.Delete()
		case "release":
			err = job.Release()
		}

		if err != nil {
			consumer.t.Fatalf("Error while calling '%s' on job: %s", command, err)
		}
	}(job, command)
}

// ****************************************************************************

func TestConsumerPlayAndPause(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()

	// By default, the consumer is paused.
	consumer.ExpectNoRequest()

	// Unpause the consumer, which should trigger a reserve.
	consumer.Play()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"

	// Pause the consumer, which shouldn't trigger a reserve.
	consumer.Pause()
	consumer.ExpectNoRequest()
}

func TestConsumerStop(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()

	if consumer.isStopped != false {
		t.Fatal("Expected consumer to not be stopped")
	}
	if !consumer.Stop() {
		t.Fatal("Expected a call to Stop() to succeed")
	}
	if consumer.isStopped != true {
		t.Fatal("Expected consumer to be stopped")
	}
	if consumer.Stop() {
		t.Fatal("Expected a call to Stop() to fail")
	}
}

func TestConsumerReserveAndDelete(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()

	// Unpause the consumer, which should trigger a reserve. Return a job.
	consumer.Play()
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// The reserved job should be offered.
	job := consumer.ExpectJobOffer()
	consumer.Expect("reserve-with-timeout 0") <- "TIMED_OUT"
	consumer.FinalizeJob(job, "delete")
	consumer.Expect("delete 1") <- "DELETED"

	// Pause the consumer, even though a reserve was already issued.
	consumer.Pause()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"
	consumer.ExpectNoRequest()
}

func TestConsumerJobReleaseOnPause(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()
	consumer.Play()

	// Reserve a job.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"

	// Pausing the consumer should trigger a release.
	consumer.Pause()
	consumer.Expect("release 1 1024 0") <- "RELEASED"
	consumer.ExpectNoRequest()
}

func TestConsumerReserveWithDeadline(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()
	consumer.Play()

	// Try to reserve 2 jobs for which a DEADLINE_SOON is issued on the 2nd
	// reserve.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectJobOffer()
	consumer.Expect("reserve-with-timeout 0") <- "DEADLINE_SOON"

	// the DEADLINE_SOON response triggers a 1 second timeout.
	time.Sleep(time.Second)

	// A DEADLINE_SOON should simply trigger a new reserve.
	consumer.Expect("reserve-with-timeout 0") <- "TIMED_OUT"
}

func TestConsumerReserveFlow(t *testing.T) {
	consumer := NewTestConsumer(t)
	defer consumer.Close()
	consumer.Play()

	// Reserve a job.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// The job should be pending and fetchable.
	job1 := consumer.ExpectJobOffer()

	// Once the job is fetched, a new reserve should have been issued.
	consumer.Expect("reserve-with-timeout 0") <- "RESERVED 2 3\r\nfoo"
	consumer.Expect("stats-job 2") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// Another job should be pending and fetchable.
	job2 := consumer.ExpectJobOffer()

	// Pause the consumer and catch the pending reserve.
	consumer.Pause()
	consumer.Expect("reserve-with-timeout 0") <- "TIMED_OUT"

	// Allow the select-statement to read the pause channel, before we let it
	// compete with the job command channel.
	time.Sleep(waitABit)

	// Release the 1st job.
	go func() {
		if err := job1.Release(); err != nil {
			t.Fatalf("Error releasing job 1: %s", err)
		}
	}()

	consumer.Expect("release 1 1024 0") <- "RELEASED"
	consumer.ExpectNoRequest()

	// Release the 2nd job.
	go func() {
		if err := job2.Release(); err != nil {
			t.Fatalf("Error releasing job 2: %s", err)
		}
	}()

	consumer.Expect("release 2 1024 0") <- "RELEASED"
	consumer.ExpectNoRequest()

	// Start the consumer back up again. This should yield a reserve with a
	// specified timeout.
	consumer.Play()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"
}
