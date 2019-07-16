package beanstalk

import (
	"context"
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

// Is the line equal to the specified string?
func (line Line) Is(s string) bool {
	return s == line.line
}

// At lineno, is the line present?
func (line Line) At(lineno int, s string) bool {
	return lineno == line.lineno && s == line.line
}

// Server implements a test beanstalk server.
type Server struct {
	listener net.Listener
	mu       sync.RWMutex
	lineno   int
	handler  func(line Line) string
}

// NewServer returns a new Server.
func NewServer() *Server {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("Unable to set up listening socket for text beanstalk server: " + err.Error())
	}

	server := &Server{listener: listener}
	go server.accept()

	return server
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

	for {
		line, err := conn.ReadLine()
		if err != nil {
			return
		}

		// Fetch a read-lock and call the handler with the line information that
		// was just read.
		func() {
			server.mu.RLock()
			defer server.mu.RUnlock()

			server.lineno++
			if server.handler != nil {
				if resp := server.handler(Line{server.lineno, line}); resp != "" {
					conn.PrintfLine(resp)
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

func TestConn(t *testing.T) {
	server := NewServer()
	defer server.listener.Close()

	// Dial the beanstalk server and set up a client connection.
	var conn *Conn
	t.Run("Dial", func(t *testing.T) {
		var err error
		conn, err = Dial(server.listener.Addr().String(), Config{})
		if err != nil {
			t.Fatalf("Unable to dial to beanstalk server: %s", err)
		}
	})
	defer conn.Close()

	// Ignore watching a tube.
	var ctx = context.Background()
	t.Run("Ignore", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			if line.Is("ignore foo") {
				return "WATCHING 1"
			}

			t.Fatalf("Unexpected client request: %s", line.line)
			return ""
		})

		// LastTube test what happens if the ignore command fails because it tried
		// to ignore the only tube this connection was watching.
		t.Run("LastTube", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				if line.Is("ignore bar") {
					return "NOT_IGNORED"

				}

				t.Fatalf("Unexpected client request: %s", line.line)
				return ""
			})

			err := conn.Ignore(ctx, "bar")
			switch {
			case err == ErrDisconnected:
			case err != ErrNotIgnored:
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
				return "INSERTED 12345"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
			}

			return ""
		})

		id, err := conn.Put(ctx, "foobar", []byte("Hello World"), PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second})
		switch {
		case err == ErrDisconnected:
		case err != nil:
			t.Fatalf("Error inserting a new job: %s", err)
		case id != 12345:
			t.Fatalf("Expected job ID 12345, but got %d", id)
		}

		// OnSameTube tests if the command order makes sense if another message is
		// put into the same tube.
		t.Run("OnSameTube", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "put 1024 10 60 11"):
				case line.At(2, "Hello World"):
					return "INSERTED 54321"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			id, err := conn.Put(ctx, "foobar", []byte("Hello World"), PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second})
			switch {
			case err == ErrDisconnected:
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 54321:
				t.Fatalf("Expected job ID 54321, but got %d", id)
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
					return "INSERTED 12345"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			id, err := conn.Put(ctx, "zoink", []byte("Hello Narf"), PutParams{Priority: 512, Delay: 15 * time.Second, TTR: 30 * time.Second})
			switch {
			case err == ErrDisconnected:
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 12345:
				t.Fatalf("Expected job ID 12345, but got %d", id)
			}
		})
	})

	// ReserveWithTimeout tests the ReserveWithTimeout method.
	t.Run("ReserveWithTimeout", func(t *testing.T) {
		server.HandleFunc(func(line Line) string {
			switch {
			case line.At(1, "reserve-with-timeout 3"):
				return "RESERVED 12 11\r\nHello World"
			case line.At(2, "stats-job 12"):
				return "OK 166\r\n---\r\nid: 12\r\ntube: default\r\nstate: reserved\r\npri: 512\r\nage: 23\r\ndelay: 15\r\nttr: 30\r\ntime-left: 25\r\nfile: 6\r\nreserves: 1\r\ntimeouts: 4\r\nreleases: 5\r\nburies: 2\r\nkicks: 7"
			default:
				t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)

			}

			return ""
		})

		job, err := conn.ReserveWithTimeout(ctx, 3*time.Second)
		switch {
		case err == ErrDisconnected:
		case err != nil:
			t.Fatalf("Error reserving a job: %s", err)
		case job == nil:
			t.Fatal("Expected job, but got nothing")
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
			case err == ErrDisconnected:
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
				case line.At(1, "reserve-with-timeout 2"):
					return "DEADLINE_SOON"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			job, err := conn.ReserveWithTimeout(ctx, 2*time.Second)
			switch {
			case err == ErrDisconnected:
			case err != nil:
				t.Fatalf("Error reserving a job: %s", err)
			case job != nil:
				t.Fatalf("Expected job to be nil, but got %#v", job)
			}
		})

		// WithNotFound tests if the situation where a reserved job expired before
		// the stats-job command could return sucessfully.
		t.Run("WithNotFound", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "reserve-with-timeout 2"):
					return "RESERVED 13 11\r\nHello World"
				case line.At(2, "stats-job 13"):
					return "NOT_FOUND"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)

				}

				return ""
			})

			job, err := conn.ReserveWithTimeout(ctx, 2*time.Second)
			switch {
			case err == ErrDisconnected:
			case err != nil:
				t.Fatalf("Error reserving a job: %s", err)
			case job != nil:
				t.Fatalf("Expected job to be nil, but got %#v", job)
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

		err := conn.Watch(ctx, "events")
		switch {
		case err == ErrDisconnected:
		case err != nil:
			t.Fatalf("Error watching a channel: %s", err)
		}

		// ErrTubeTooLong tests if a client-side error is returned if the tube name
		// is too long.
		t.Run("ErrTubeTooLong", func(t *testing.T) {
			name := make([]byte, 201)
			for i := 0; i < len(name); i++ {
				name[i] = 'a'
			}

			err := conn.Watch(ctx, string(name))
			switch {
			case err == ErrDisconnected:
			case err != ErrTubeTooLong:
				t.Fatalf("Expected error %s, but go %s", ErrTubeTooLong, err)
			}
		})
	})
}
