package beanstalk

import (
	"net"
	"net/textproto"
	"os"
	"syscall"
	"testing"
	"time"
)

func socketpair() (net.Conn, net.Conn, error) {
	fds, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}

	fd1 := os.NewFile(uintptr(fds[0]), "fd1")
	defer fd1.Close()

	fd2 := os.NewFile(uintptr(fds[1]), "fd2")
	defer fd2.Close()

	sock1, err := net.FileConn(fd1)
	if err != nil {
		return nil, nil, err
	}

	sock2, err := net.FileConn(fd2)
	if err != nil {
		sock1.Close()
		return nil, nil, err
	}

	return sock1, sock2, nil
}

type TestClient struct {
	*Client
	ServerConn *textproto.Conn
}

func NewTestClient(t *testing.T, req, resp string) *TestClient {
	s1, s2, err := socketpair()
	if err != nil {
		panic("Unable to create socket pair: " + err.Error())
	}

	client := &TestClient{Client: NewClient(s1, DefaultOptions()), ServerConn: textproto.NewConn(s2)}
	go client.ServerResponse(t, req, resp)

	return client
}

func (tc *TestClient) ServerResponse(t *testing.T, req, resp string) {
	defer tc.ServerConn.Close()

	line, err := tc.ServerConn.ReadLine()
	if err != nil {
		t.Fatalf("Unable to read line from client: %s", err)
	}

	if line != req {
		t.Fatalf("Expected client request to be '%s', but got '%s'", req, line)
	}

	if err := tc.ServerConn.PrintfLine(resp); err != nil {
		t.Fatalf("Unable to send response to client: %s", err)
	}
}

// ****************************************************************************

func TestBury(t *testing.T) {
	client := NewTestClient(t, "bury 12345 9876", "BURIED")
	defer client.Close()

	if err := client.Bury(&Job{ID: 12345}, 9876); err != nil {
		t.Fatalf("Unexpected error from Bury: %s", err)
	}
}

func TestBuryNotFound(t *testing.T) {
	client := NewTestClient(t, "bury 12345 9876", "NOT_FOUND")
	defer client.Close()

	if err := client.Bury(&Job{ID: 12345}, 9876); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, but got: %s", err)
	}
}

func TestDelete(t *testing.T) {
	client := NewTestClient(t, "delete 12345", "DELETED")
	defer client.Close()

	if err := client.Delete(&Job{ID: 12345}); err != nil {
		t.Fatalf("Unexpected error from Delete: %s", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	client := NewTestClient(t, "delete 12345", "NOT_FOUND")
	defer client.Close()

	if err := client.Delete(&Job{ID: 12345}); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, but got: %s", err)
	}
}

func TestIgnore(t *testing.T) {
	client := NewTestClient(t, "ignore default", "WATCHING 1")
	defer client.Close()

	if err := client.Ignore("default"); err != nil {
		t.Fatalf("Unexpected error from Ignore: %s", err)
	}
}

func TestIgnoreNotIgnored(t *testing.T) {
	client := NewTestClient(t, "ignore default", "NOT_IGNORED")
	defer client.Close()

	if err := client.Ignore("default"); err != ErrNotIgnored {
		t.Fatalf("Expected ErrNotIgnored, but got: %s", err)
	}
}

func TestPut(t *testing.T) {
	client := NewTestClient(t, "put 1024 0 10 11", "INSERTED 12345")
	defer client.Close()

	job := &PutRequest{Body: []byte("Hello World"), Tube: "test", Params: &PutParams{1024, 0, time.Duration(10 * time.Second)}}

	id, err := client.Put(job)
	if err != nil {
		t.Fatalf("Unexpected error from Put: %s", err)
	}
	if id != 12345 {
		t.Fatalf("Unexpected ID from Put. Expected 12345, but got %d", id)
	}
}

func TestPutBuried(t *testing.T) {
	client := NewTestClient(t, "put 1024 0 10 11", "BURIED 12345")
	defer client.Close()

	job := &PutRequest{Body: []byte("Hello World"), Tube: "test", Params: &PutParams{1024, 0, time.Duration(10 * time.Second)}}

	id, err := client.Put(job)
	if err != ErrBuried {
		t.Fatalf("Expected ErrBuried, but got: %s", err)
	}
	if id != 12345 {
		t.Fatalf("Unexpected ID from Put. Expected 12345, but got %d", id)
	}
}

func TestRelease(t *testing.T) {
	client := NewTestClient(t, "release 12345 1024 10", "RELEASED")
	defer client.Close()

	if err := client.Release(&Job{ID: 12345}, 1024, time.Duration(10*time.Second)); err != nil {
		t.Fatalf("Unexpected error from Release: %s", err)
	}
}

func TestReleaseBuried(t *testing.T) {
	client := NewTestClient(t, "release 12345 1024 10", "BURIED")
	defer client.Close()

	if err := client.Release(&Job{ID: 12345}, 1024, time.Duration(10*time.Second)); err != ErrBuried {
		t.Fatalf("Expected ErrBuried, but got: %s", err)
	}
}

func TestReleaseNotFound(t *testing.T) {
	client := NewTestClient(t, "release 12345 1024 10", "NOT_FOUND")
	defer client.Close()

	if err := client.Release(&Job{ID: 12345}, 1024, time.Duration(10*time.Second)); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, but got: %s", err)
	}
}

func TestReserve(t *testing.T) {
	client := NewTestClient(t, "reserve-with-timeout 1", "RESERVED 1234 11\r\nHello World")
	defer client.Close()

	job, err := client.Reserve(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error from Reserve: %s", err)
	}
	if job == nil {
		t.Fatal("Expected a job from Reserve, but got nothing")
	}
}

func TestReserveTimedOut(t *testing.T) {
	client := NewTestClient(t, "reserve-with-timeout 2", "TIMED_OUT")
	defer client.Close()

	job, err := client.Reserve(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error from Reserve: %s", err)
	}
	if job != nil {
		t.Fatalf("Expected no job from Reserve, but got job: %v", job)
	}
}

func TestTouch(t *testing.T) {
	client := NewTestClient(t, "touch 12345", "TOUCHED")
	defer client.Close()

	if err := client.Touch(&Job{ID: 12345}); err != nil {
		t.Fatalf("Unexpected error from Touch: %s", err)
	}

}

func TestTouchNotFound(t *testing.T) {
	client := NewTestClient(t, "touch 12345", "NOT_FOUND")
	defer client.Close()

	if err := client.Touch(&Job{ID: 12345}); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, but got: %s", err)
	}
}

func TestUse(t *testing.T) {
	client := NewTestClient(t, "use test", "USING test")
	defer client.Close()

	if err := client.Use("test"); err != nil {
		t.Fatalf("Unexpected error from Use: %s", err)
	}
}

func TestWatch(t *testing.T) {
	client := NewTestClient(t, "watch test", "WATCHING test")
	defer client.Close()

	if err := client.Watch("test"); err != nil {
		t.Fatalf("Unexpected error from Watch: %s", err)
	}
}
