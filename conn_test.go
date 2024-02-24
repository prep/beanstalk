package beanstalk

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/textproto"
	"sync"
	"testing"
	"time"
)

// Line describes a read line from the client.
type Line struct {
	lineno int
	line   string
}

// At validates if the specified string is present at a specific line number.
func (line Line) At(lineno int, s string) bool {
	return lineno == line.lineno && s == line.line
}

// Server implements a test beanstalk server.
type Server struct {
	listener  net.Listener
	mu        sync.RWMutex
	lineno    int
	handler   func(line Line) string
	closeC    chan struct{}
	closeOnce sync.Once
}

// NewServer returns a new Server.
func NewServer() *Server {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("Unable to set up listening socket for text beanstalk server: " + err.Error())
	}

	server := &Server{listener: listener, closeC: make(chan struct{})}
	go server.accept()

	return server
}

// Close the server socket.
func (server *Server) Close() {
	server.closeOnce.Do(func() {
		close(server.closeC)
		_ = server.listener.Close()
	})
}

// accept incoming connections.
func (server *Server) accept() {
	defer server.listener.Close()

	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}

		server.handleConn(textproto.NewConn(conn))
	}
}

// handleConn handles an existing client connection.
func (server *Server) handleConn(conn *textproto.Conn) {
	defer conn.Close()

	lineC := make(chan string)

	// Read incoming lines and send them to lineC.
	go func() {
		for {
			line, err := conn.ReadLine()
			if err != nil {
				close(lineC)
				return
			}

			select {
			case lineC <- line:
			case <-server.closeC:
				return
			}
		}
	}()

	// Process incoming lines and send them to the configured handler.
	for {
		var line string
		var closed bool

		select {
		case line, closed = <-lineC:
			if !closed {
				return
			}

		case <-server.closeC:
			return
		}

		// Execute this in an inline function so that the lock/unlock mechanism
		// is handled elegantly.
		func() {
			server.mu.RLock()
			defer server.mu.RUnlock()

			server.lineno++
			if server.handler != nil {
				if resp := server.handler(Line{server.lineno, line}); resp != "" {
					_ = conn.PrintfLine(resp)
				}
			}
		}()
	}
}

// HandleFunc registers the handler function that should be called for every
// line that this server receives from the client.
func (server *Server) HandleFunc(handler func(line Line) string) {
	server.mu.Lock()
	defer server.mu.Unlock()

	server.lineno = 0
	server.handler = handler
}

// Socket returns the host:port combo that this server is listening on.
func (server *Server) Socket() string {
	return server.listener.Addr().String()
}

func TestConn(t *testing.T) {
	server := NewServer()
	defer server.Close()

	var conn *Conn
	ctx := context.Background()

	// Dial the beanstalk server and set up a client connection.
	t.Run("Dial", func(t *testing.T) {
		var err error
		conn, err = Dial(server.Socket(), Config{})
		if err != nil {
			t.Fatalf("Unable to dial to beanstalk server: %s", err)
		}
	})
	defer conn.Close()

	// bury a job.
	t.Run("bury", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "bury 1 10"):
				return "BURIED"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		if err := conn.Bury(ctx, &Job{ID: 1}, 10); err != nil {
			t.Fatalf("Error burying job: %s", err)
		}

		// NotFound tests what happens when the NOT_FOUND error is returned.
		t.Run("NotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "bury 2 11"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			err := conn.Bury(ctx, &Job{ID: 2}, 11)
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				t.Fatalf("Error burying job: %s", err)
			}
		})
	})

	// delete a job.
	t.Run("Delete", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "delete 3"):
				return "DELETED"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		if err := conn.Delete(ctx, 3); err != nil {
			t.Fatalf("Error deleting job: %s", err)
		}

		// NotFound tests what happens when the NOT_FOUND error is returned.
		t.Run("NotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "delete 4"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			err := conn.Delete(ctx, 4)
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				t.Fatalf("Error deleting job: %s", err)
			}
		})
	})

	// Ignore watching a tube.
	t.Run("Ignore", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			if line.At(1, "ignore foo") {
				return "WATCHING 1"
			}

			t.Fatalf("Unexpected client request: %s", line.line)
			return ""
		})

		if err := conn.Ignore(ctx, "foo"); err != nil {
			t.Fatalf("Error ignoring tube: %s", err)
		}

		// NotIgnored test what happens if the ignore command fails because it tried
		// to ignore the only tube this connection was watching.
		t.Run("NotIgnored", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				if line.At(1, "ignore bar") {
					return "NOT_IGNORED"
				}

				t.Fatalf("Unexpected client request: %s", line.line)
				return ""
			})

			err := conn.Ignore(ctx, "bar")
			switch {
			case errors.Is(err, ErrNotIgnored):
			case err != nil:
				t.Fatalf("Error ignoring tube: %s", err)
			}
		})
	})

	// Put a new message into a tube.
	t.Run("Put", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "use foobar"):
				return "USING foobar"
			case line.At(2, "put 1024 10 60 11"):
			case line.At(3, "Hello World"):
				return "INSERTED 5"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		id, err := conn.Put(ctx, "foobar", []byte("Hello World"), PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second})
		switch {
		case err != nil:
			t.Fatalf("Error inserting a new job: %s", err)
		case id != 5:
			t.Fatalf("Expected job ID 5, but got %d", id)
		}

		// OnSameTube tests if the command order makes sense if another message is
		// put into the same tube.
		t.Run("OnSameTube", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "put 1024 10 60 11"):
				case line.At(2, "Hello World"):
					return "INSERTED 6"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			id, err := conn.Put(ctx, "foobar", []byte("Hello World"), PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second})
			switch {
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 6:
				t.Fatalf("Expected job ID 6, but got %d", id)
			}
		})

		// OnDifferentTube tests if the command order makes sense if a message is
		// put into a different tube than the previous message.
		t.Run("OnDifferentTube", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "use zoink"):
					return "USING zoink"
				case line.At(2, "put 512 15 30 10"):
				case line.At(3, "Hello Narf"):
					return "INSERTED 7"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			id, err := conn.Put(ctx, "zoink", []byte("Hello Narf"), PutParams{Priority: 512, Delay: 15 * time.Second, TTR: 30 * time.Second})
			switch {
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 7:
				t.Fatalf("Expected job ID 7, but got %d", id)
			}
		})
	})

	// release tests the release method, responsible for releasing jobs back.
	t.Run("release", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "release 8 12 20"):
				return "RELEASED"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		err := conn.Release(ctx, &Job{ID: 8}, 12, 20*time.Second)
		if err != nil {
			t.Fatalf("Error releasing job: %s", err)
		}

		// Buried tests what happens when the BURIED error is returned.
		t.Run("Buried", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "release 9 13 21"):
					return "BURIED"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			err = conn.Release(ctx, &Job{ID: 9}, 13, 21*time.Second)
			switch {
			case errors.Is(err, ErrBuried):
			case err != nil:
				t.Fatalf("Expected the ErrBuried error, but got %s", err)
			}
		})

		// NotFound tests what happens when the NOT_FOUND error is returned.
		t.Run("NotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "release 10 14 22"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			err = conn.Release(ctx, &Job{ID: 10}, 14, 22*time.Second)
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				t.Fatalf("Expected the ErrNotFound error, but got %s", err)
			}
		})
	})

	// ReserveWithTimeout tests the ReserveWithTimeout method.
	t.Run("ReserveWithTimeout", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "reserve-with-timeout 1"):
				return "RESERVED 12 11\r\nHello World"
			case line.At(2, "stats-job 12"):
				return "OK 166\r\n---\r\nid: 12\r\ntube: default\r\nstate: reserved\r\npri: 512\r\nage: 23\r\ndelay: 15\r\nttr: 30\r\ntime-left: 25\r\nfile: 6\r\nreserves: 1\r\ntimeouts: 4\r\nreleases: 5\r\nburies: 2\r\nkicks: 7"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		job, err := conn.ReserveWithTimeout(ctx, 1*time.Second)
		switch {
		case err != nil:
			t.Fatalf("Error reserving a job: %s", err)
		case job == nil:
			t.Fatal("Expected job, but got nothing")

		// Validate the basic attributes.
		case job.ID != 12:
			t.Fatalf("Expected job ID 12, but got %d", job.ID)
		case !bytes.Equal(job.Body, []byte(`Hello World`)):
			t.Fatalf("Expected job body to be \"Hello World\", but got %q", string(job.Body))
		case job.ReservedAt.IsZero():
			t.Fatal("Expected job ReservedAt to be set, but it was not")

		// Validate the Stats.
		case job.Stats.Tube != "default":
			t.Fatalf("Expected job tube default, but got %s", job.Stats.Tube)
		case job.Stats.State != "reserved":
			t.Fatalf("Expected job state reserved, but got %s", job.Stats.State)
		case job.Stats.Age != 23*time.Second:
			t.Fatalf("Expected job age to be 23s, but got %s", job.Stats.Age)
		case job.Stats.TimeLeft != 25*time.Second:
			t.Fatalf("Expected job time left to be 25s, but got %s", job.Stats.TimeLeft)
		case job.Stats.File != 6:
			t.Fatalf("Expected job binfile number to be 6, but got %d", job.Stats.File)
		case job.Stats.Reserves != 1:
			t.Fatalf("Expected job reserved to be 1, but got %d", job.Stats.Reserves)
		case job.Stats.Timeouts != 4:
			t.Fatalf("Expected job timeouts to be 4, but got %d", job.Stats.Timeouts)
		case job.Stats.Releases != 5:
			t.Fatalf("Expected job release to be 5, but got %d", job.Stats.Releases)
		case job.Stats.Buries != 2:
			t.Fatalf("Expected job buries to be 2, but got %d", job.Stats.Buries)
		case job.Stats.Kicks != 7:
			t.Fatalf("Expected job kicks to be 7, but got %d", job.Stats.Kicks)

		// Validate the PutParams.
		case job.Stats.PutParams.Priority != 512:
			t.Fatalf("Expected job priority to be 512, but got %d", job.Stats.PutParams.Priority)
		case job.Stats.PutParams.Delay != 15*time.Second:
			t.Fatalf("Expected job TTR to be 15s, but got %s", job.Stats.PutParams.Delay)
		case job.Stats.PutParams.TTR != 30*time.Second:
			t.Fatalf("Expected job TTR to be 30s, but got %s", job.Stats.PutParams.TTR)
		}

		// WithTimeout tests if a TIMED_OUT response is properly handled.
		t.Run("WithTimeout", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "reserve-with-timeout 2"):
					return "TIMED_OUT"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job, err := conn.ReserveWithTimeout(ctx, 2*time.Second)
			switch {
			case err != nil:
				t.Fatalf("Error reserving a job: %s", err)
			case job != nil:
				t.Fatalf("Expected job to be nil, but got %#v", job)
			}
		})

		// WithDeadline tests if a DEADLINE_SOON response is properly handled.
		t.Run("WithDeadline", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "reserve-with-timeout 3"):
					return "DEADLINE_SOON"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job, err := conn.ReserveWithTimeout(ctx, 3*time.Second)
			switch {
			case err != nil:
				t.Fatalf("Error reserving a job: %s", err)
			case job != nil:
				t.Fatalf("Expected job to be nil, but got %#v", job)
			}
		})

		// WithNotFound tests if the situation where a reserved job expired before
		// the stats-job command could return successfully.
		t.Run("WithNotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "reserve-with-timeout 4"):
					return "RESERVED 13 11\r\nHello World"
				case line.At(2, "stats-job 13"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job, err := conn.ReserveWithTimeout(ctx, 4*time.Second)
			switch {
			case err != nil:
				t.Fatalf("Error reserving a job: %s", err)
			case job != nil:
				t.Fatalf("Expected job to be nil, but got %#v", job)
			}
		})
	})

	// touch an existing job.
	t.Run("touch", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "touch 13"):
				return "TOUCHED"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		job := &Job{ID: 13}
		job.Stats.PutParams.TTR = 5 * time.Second

		err := conn.Touch(ctx, job)
		switch {
		case err != nil:
			t.Fatalf("Error watching a channel: %s", err)
		case job.Stats.PutParams.TTR != 5*time.Second:
			t.Fatalf("Expected job TTR to be 5s, but got %s", job.Stats.PutParams.TTR)
		case job.Stats.TimeLeft != 4*time.Second:
			t.Fatalf("Expected job time left to be 4s, but got %s", job.Stats.TimeLeft)
		case job.ReservedAt.IsZero():
			t.Fatal("Expected job ReservedAt to be set, but it was not")
		}

		t.Run("NotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "touch 14"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			err = conn.Touch(ctx, &Job{ID: 14})
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				t.Fatalf("Expected the ErrNotFound error, but got %s", err)
			}
		})
	})

	// Watch a new tube.
	t.Run("Watch", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "watch events"):
				return "WATCHING 2"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		if err := conn.Watch(ctx, "events"); err != nil {
			t.Fatalf("Error watching a channel: %s", err)
		}

		// ErrTubeTooLong tests if a client-side error is returned if the tube name
		// is too long.
		t.Run("ErrTubeTooLong", func(t *testing.T) {
			err := conn.Watch(ctx, string(make([]byte, 201)))
			switch {
			case errors.Is(err, ErrTubeTooLong):
			case err != nil:
				t.Fatalf("Expected the ErrTubeTooLong error, but got %s", err)
			}
		})
	})

	t.Run("Kick", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "use default"):
				return "USING default"
			case line.At(2, "kick 10"):
				return "KICKED 10"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		count, err := conn.Kick(ctx, "default", 10)
		if err != nil {
			t.Fatalf("Error kicking jobs: %s", err)
		}

		if count != 10 {
			t.Fatalf("Unexpected number of kicked jobs, expected %d, actual %d", 10, count)
		}

		t.Run("NoBuriedJobs", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "kick 10"):
					return "KICKED 0"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			count, err := conn.Kick(ctx, "default", 10)
			switch {
			case err != nil:
				t.Fatalf("Error kicking job: %s", err)
			case count != 0:
				t.Fatalf("Unexpected number of kicked jobs, expected %d, actual %d", 0, count)
			}
		})
	})

	t.Run("kick", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "kick-job 1"):
				return "KICKED"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		job := Job{ID: 1}
		job.Stats.Tube = "default"

		if err := conn.KickJob(ctx, &job); err != nil {
			t.Fatalf("Error kicking job: %s", err)
		}

		// NotFound tests what happens when the NOT_FOUND error is returned.
		t.Run("JobNotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "kick-job 1"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job := Job{ID: 1}
			job.Stats.Tube = "default"

			err := conn.KickJob(ctx, &job)
			switch {
			case errors.Is(err, ErrNotFound):
			case err != nil:
				t.Fatalf("Error kicking job: %s", err)
			}
		})
	})

	t.Run("PeekDelayed", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "use events"):
				return "USING events"
			case line.At(2, "peek-delayed"):
				return "FOUND 1 11\nHello world"
			case line.At(3, "stats-job 1"):
				return "OK 153\n---\nid: 1\ntube: default\nstate: delayed\npri: 1024\nage: 39\ndelay: 120\nttr: 60\ntime-left: 80\nfile: 6\nreserves: 1\ntimeouts: 4\nreleases: 5\nburies: 2\nkicks: 7\n"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		job, err := conn.PeekDelayed(ctx, "events")
		switch {
		case err != nil:
			t.Fatalf("Error picking delayed job: %s", err)
		case job == nil:
			t.Fatal("Expected job, but got nothing")

		// Validate the basic attributes.
		case job.ID != 1:
			t.Fatalf("Unexpected job ID. Expected: 1, actual: %d", job.ID)
		case !bytes.Equal(job.Body, []byte(`Hello world`)):
			t.Fatalf("Expected job body to be \"Hello world\", but got %q", string(job.Body))

		// Validate the Stats.
		case job.Stats.Tube != "default":
			t.Fatalf("Expected job tube default, but got %s", job.Stats.Tube)
		case job.Stats.State != "delayed":
			t.Fatalf("Expected job state delayed, but got %s", job.Stats.State)
		case job.Stats.Age != 39*time.Second:
			t.Fatalf("Expected job age to be 39s, but got %s", job.Stats.Age)
		case job.Stats.TimeLeft != 80*time.Second:
			t.Fatalf("Expected job time left to be 80s, but got %s", job.Stats.TimeLeft)
		case job.Stats.File != 6:
			t.Fatalf("Expected job binfile number to be 6, but got %d", job.Stats.File)
		case job.Stats.Reserves != 1:
			t.Fatalf("Expected job reserved to be 1, but got %d", job.Stats.Reserves)
		case job.Stats.Timeouts != 4:
			t.Fatalf("Expected job timeouts to be 4, but got %d", job.Stats.Timeouts)
		case job.Stats.Releases != 5:
			t.Fatalf("Expected job release to be 5, but got %d", job.Stats.Releases)
		case job.Stats.Buries != 2:
			t.Fatalf("Expected job buries to be 2, but got %d", job.Stats.Buries)
		case job.Stats.Kicks != 7:
			t.Fatalf("Expected job kicks to be 7, but got %d", job.Stats.Kicks)

		// Validate the PutParams.
		case job.Stats.PutParams.Priority != 1024:
			t.Fatalf("Expected job priority to be 1024, but got %d", job.Stats.PutParams.Priority)
		case job.Stats.PutParams.Delay != 120*time.Second:
			t.Fatalf("Expected job TTR to be 120s, but got %s", job.Stats.PutParams.Delay)
		case job.Stats.PutParams.TTR != 60*time.Second:
			t.Fatalf("Expected job TTR to be 60s, but got %s", job.Stats.PutParams.TTR)
		}

		t.Run("JobNotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "peek-delayed"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job, err := conn.PeekDelayed(ctx, "events")
			switch {
			case err != nil:
				t.Fatalf("Error picking delayed job: %s", err)
			case job != nil:
				t.Fatalf("Expected job not found, but got something: %+v", job)
			}
		})
	})
}
