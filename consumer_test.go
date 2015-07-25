package beanstalk

import (
	"net"
	"net/textproto"
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

func NewTestConsumer(t *testing.T, window int) *TestConsumer {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic("Unable to create consumer socket: " + err.Error())
	}

	jobC := make(chan *Job)

	options := &Options{
		QueueSize:        window,
		ReserveTimeout:   time.Second,
		ReconnectTimeout: time.Second * 3,
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
	consumer := NewTestConsumer(t, 1)
	defer consumer.Close()

	// Unpause the consumer, which should trigger a reserve.
	consumer.Play()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"

	// Pause the consumer, which shouldn't trigger a reserve.
	consumer.Pause()
	consumer.ExpectNoRequest()
}

func TestConsumerReserveAndDelete(t *testing.T) {
	consumer := NewTestConsumer(t, 1)
	defer consumer.Close()

	// Unpause the consumer, which should trigger a reserve. Return a job.
	consumer.Play()
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// The reserved job should be offered.
	job := consumer.ExpectJobOffer()
	consumer.FinalizeJob(job, "delete")
	consumer.Expect("delete 1") <- "DELETED"

	// Pause the consumer, even though a reserve was already issued.
	consumer.Pause()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"
	consumer.ExpectNoRequest()
}

func TestConsumerReserveWithQueueSizeOf2(t *testing.T) {
	consumer := NewTestConsumer(t, 2)
	defer consumer.Close()
	consumer.Play()

	if !consumer.queue.IsEmpty() {
		t.Fatal("Expected queue to be empty")
	}

	// The 1st reserve should have a timeout.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"

	// The 2nd reserve shouldn't have a timeout.
	consumer.Expect("reserve-with-timeout 0") <- "RESERVED 2 3\r\nbar"
	consumer.Expect("stats-job 2") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	if !consumer.queue.IsFull() {
		t.Fatal("Expected queue to be full")
	}
}

func TestConsumerJobReleaseOnPause(t *testing.T) {
	consumer := NewTestConsumer(t, 1)
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

func TestConsumerJobReleaseOnPauseWithQueueSize2(t *testing.T) {
	consumer := NewTestConsumer(t, 2)
	defer consumer.Close()
	consumer.Play()

	// Reserve 2 jobs.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.Expect("reserve-with-timeout 0") <- "RESERVED 2 3\r\nbar"
	consumer.Expect("stats-job 2") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// Pausing the consumer should trigger a release.
	consumer.Pause()
	consumer.Expect("release 1 1024 0") <- "RELEASED"
	consumer.Expect("release 2 1024 0") <- "RELEASED"
	consumer.ExpectNoRequest()
}

func TestConsumerJobReleaseOnPauseWithQueueSize2And1Offered(t *testing.T) {
	consumer := NewTestConsumer(t, 2)
	defer consumer.Close()
	consumer.Play()

	// Reserve 2 jobs.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.Expect("reserve-with-timeout 0") <- "RESERVED 2 3\r\nbar"
	consumer.Expect("stats-job 2") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.ExpectNoRequest()

	// Fetch 1 job so it gets marked internally as 'offered'.
	if job := consumer.ExpectJobOffer(); job.ID != 1 {
		t.Fatalf("Expected job with ID=1, but got ID=%d", job.ID)
	}

	// Pausing the consumer should trigger a release.
	consumer.Pause()
	consumer.Expect("release 2 1024 0") <- "RELEASED"
	consumer.ExpectNoRequest()
}

func TestConsumerQueueOnReconnect(t *testing.T) {
	consumer := NewTestConsumer(t, 2)
	defer consumer.Close()
	consumer.Play()

	// Reserve a job.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"

	// Close the client socket.
	consumer.client.Close()
	time.Sleep(waitABit)

	// The queue should be empty.
	if !consumer.queue.IsEmpty() {
		t.Fatal("Expected queue to be empty")
	}

	// After the reconnect the consumer will first issue its watch and ignore
	// commands, before starting with a new reserve.
	consumer.ExpectWatchAndIgnore()
	consumer.Expect("reserve-with-timeout 1") <- "TIMED_OUT"
}

func TestConsumerReserveWithDeadline(t *testing.T) {
	consumer := NewTestConsumer(t, 2)
	defer consumer.Close()
	consumer.Play()

	// Try to reserve 2 jobs for which a DEADLINE_SOON is issued on the 2nd
	// reserve.
	consumer.Expect("reserve-with-timeout 1") <- "RESERVED 1 3\r\nfoo"
	consumer.Expect("stats-job 1") <- "OK 29\r\n---\n    pri: 1024\n    ttr: 3\n"
	consumer.Expect("reserve-with-timeout 0") <- "DEADLINE_SOON"

	// the DEADLINE_SOON response triggers a 1 second timeout.
	time.Sleep(time.Second)

	// A DEADLINE_SOON should simply trigger a new reserve.
	consumer.Expect("reserve-with-timeout 0") <- "TIMED_OUT"
}
