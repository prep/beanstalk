package beanstalk

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	server := NewServer()
	defer server.Close()

	var producer *Producer
	defer func() {
		if producer != nil {
			producer.Stop()
		}
	}()

	// Create a new producer.
	t.Run("NewProducer", func(t *testing.T) {
		var err error
		producer, err = NewProducer([]string{server.Socket()}, Config{ReconnectTimeout: 1 * time.Microsecond})
		if err != nil {
			t.Fatalf("Unable to create a new producer: %s", err)
		}

		time.Sleep(10 * time.Millisecond)
		if !producer.IsConnected() {
			t.Fatalf("Producer was unable to connect")
		}

		// Create a new producer that connects to an invalid socket.
		t.Run("WithInvalidSocket", func(t *testing.T) {
			p, err := NewProducer([]string{"127.0.0.2:10311"}, Config{})
			if err != nil {
				t.Fatalf("Unable to create a new producer: %s", err)
			}
			defer p.Stop()

			time.Sleep(10 * time.Millisecond)
			if p.IsConnected() {
				t.Fatal("Producer to unknown socket was connected")
			}
		})

		// Test if a producer works when it has both a connected and disconnected
		// connection.
		t.Run("WithDegradedConnections", func(t *testing.T) {
			p, err := NewProducer([]string{server.Socket(), "127.0.0.2:10311"}, Config{})
			if err != nil {
				t.Fatalf("Unable to create a new producer: %s", err)
			}
			defer p.Stop()

			time.Sleep(10 * time.Millisecond)
			if !producer.IsConnected() {
				t.Fatalf("Producer was unable to connect")
			}
		})
	})

	ctx := context.Background()

	// Put a new job into a tube.
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

		params := PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second}

		id, err := producer.Put(ctx, "foobar", []byte("Hello World"), params)
		switch {
		case err != nil:
			t.Fatalf("Error inserting a new job: %s", err)
		case id != 5:
			t.Fatalf("Expected job ID 5, but got %d", id)
		}

		// Check if ErrTooBig is returned when a job is too big.
		t.Run("ErrTooBig", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "put 1024 10 60 11"):
				case line.At(2, "Hello World"):
					return "JOB_TOO_BIG"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			_, err = producer.Put(ctx, "foobar", []byte("Hello World"), params)
			if !errors.Is(err, ErrTooBig) {
				t.Fatalf("Expected JOB_TOO_BIG error, but got %s", err)
			}
		})

		// Check if ErrDisconnected is returned when an error occurs while putting
		// a job into a tube.
		t.Run("WithError", func(t *testing.T) {
			server.HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "put 1024 10 60 11"):
				case line.At(2, "Hello World"):
					return "FOOBAR"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			_, err = producer.Put(ctx, "foobar", []byte("Hello World"), params)
			if !errors.Is(err, ErrDisconnected) {
				t.Fatalf("Expected %v error, but got %s", ErrDisconnected, err)
			}
		})
	})

	// Check if Stop properly stops the producer connection.
	t.Run("Stop", func(t *testing.T) {
		time.Sleep(10 * time.Millisecond)
		if !producer.IsConnected() {
			t.Fatal("Expected producer to be connected")
		}

		producer.Stop()

		if producer.IsConnected() {
			t.Fatalf("Expected producer to be disconnected")
		}
	})
}

// TestProducerPool tests the Producer with more than 1 connection.
func TestProducerPool(t *testing.T) {
	var servers [3]*Server

	servers[0] = NewServer()
	defer servers[0].Close()

	servers[1] = NewServer()
	defer servers[1].Close()

	servers[2] = NewServer()
	defer servers[2].Close()

	// Create the producer.
	producer, err := NewProducer([]string{servers[0].Socket(), servers[1].Socket(), servers[2].Socket()}, Config{})
	if err != nil {
		t.Fatalf("Unable to create a new producer: %s", err)
	}
	defer producer.Stop()

	// Test that at least 1 connection is connected.
	time.Sleep(10 * time.Millisecond)
	if !producer.IsConnected() {
		t.Fatal("Expected producer to be connected")
	}

	params := PutParams{Priority: 1024, Delay: 10 * time.Second, TTR: 60 * time.Second}

	// Set the seeder up in such a way that 3 calls to Put will select the 1st,
	// 2nd and 3rd server in that order.
	rand.Seed(76)

	// Test if 3 calls to Put will result in the 3 servers being called.
	for i, server := range servers {
		t.Run(fmt.Sprintf("PutOnServer%d", i+1), func(t *testing.T) {
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

			id, err := producer.Put(context.Background(), "foobar", []byte("Hello World"), params)
			switch {
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 5:
				t.Fatalf("Expected job ID 5, but got %d", id)
			}
		})
	}

	// Test if 1 or 2 connections out of 3 connections in the pool makes Put
	// still work.
	for i, server := range servers[:2] {
		// Set the seeder up in such a way that a Put call with try to select the
		// 1st, 2nd and 3rd server in that order.
		rand.Seed(1)

		t.Run(fmt.Sprintf("PutWith%dServersDisconnected", i+1), func(t *testing.T) {
			server.Close()

			// Since server is closed, server+1 should get the Put request.
			servers[i+1].HandleFunc(func(line Line) string {
				switch {
				case line.At(1, "put 1024 10 60 11"):
				case line.At(2, "Hello World"):
					return "INSERTED 5"
				default:
					t.Fatalf("Unexpected client request at line %d: %s", line.lineno, line.line)
				}

				return ""
			})

			id, err := producer.Put(context.Background(), "foobar", []byte("Hello World"), params)
			switch {
			case err != nil:
				t.Fatalf("Error inserting a new job: %s", err)
			case id != 5:
				t.Fatalf("Expected job ID 5, but got %d", id)
			}

			// Test if the Producer is still marked as connected.
			t.Run("IsConnected", func(t *testing.T) {
				if !producer.IsConnected() {
					t.Fatal("Expected producer to be connected")
				}
			})
		})
	}

	t.Run("PutWithAllServersDisconnected", func(t *testing.T) {
		servers[2].Close()

		_, err = producer.Put(context.Background(), "foobar", []byte("Hello World"), params)
		switch {
		case errors.Is(err, ErrDisconnected):
		case err != nil:
			t.Fatalf("Error inserting a new job: %s", err)
		}

		// Test if the Producer is still marked as connected.
		t.Run("IsConnected", func(t *testing.T) {
			if producer.IsConnected() {
				t.Fatal("Expected producer to be disconnected")
			}
		})
	})
}
