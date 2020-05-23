package beanstalk

import (
	"context"
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

		// Check if ErrDisconnected is returned when an error occurs while putting
		// a job into a tube.
		t.Run("WithError", func(t *testing.T) {
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
			if err != ErrDisconnected {
				t.Fatalf("Expected JOB_TOO_BIG error, but got %s", err)
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
