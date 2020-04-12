package beanstalk

import (
	"crypto/tls"
	"time"
)

// A Config structure is used to configure a Consumer, Producer, one of its
// pools or Conn.
type Config struct {
	// Multiply the list of URIs specified to the producer pool or consumer.
	// The effect of this is more TCP connections being set up to load balance
	// traffic over.
	// The default is to have 1.
	Multiply int
	// NumGoroutines is the number of goroutines that the Receive() method will
	// spin up.
	// The default is to spin up 1 goroutine.
	NumGoroutines int
	// ConnTimeout configures the read and write timeout of the connection. This
	// can be overridden by a context deadline if its value is lower.
	// Note that this does not work with Reserve and might interfere with
	// ReserveWithTimeout if configured incorrectly.
	// The default is to have no timeout.
	ConnTimeout time.Duration
	// ReserveTimeout is the time a consumer should wait before reserving a job,
	// when the last attempt didn't yield a job.
	// The default is to wait 1 seconds.
	ReserveTimeout time.Duration
	// ReconnectTimeout is the timeout between reconnects.
	// The default is to wait 3 seconds.
	ReconnectTimeout time.Duration
	// TLSConfig describes the configuration that is used when Dial() makes a
	// TLS connection.
	TLSConfig *tls.Config
	// InfoFunc is called to log informational messages.
	InfoFunc func(message string)
	// ErrorFunc is called to log error messages.
	ErrorFunc func(err error, message string)
}

func (config Config) normalize() Config {
	if config.Multiply < 1 {
		config.Multiply = 1
	}
	if config.NumGoroutines < 1 {
		config.NumGoroutines = 1
	}
	if config.ConnTimeout < 0 {
		config.ConnTimeout = 0
	}
	if config.ReserveTimeout <= 0 {
		config.ReserveTimeout = 1 * time.Second
	}
	if config.ReconnectTimeout <= 0 {
		config.ReconnectTimeout = 3 * time.Second
	}
	if config.InfoFunc == nil {
		config.InfoFunc = func(string) {}
	}
	if config.ErrorFunc == nil {
		config.ErrorFunc = func(error, string) {}
	}

	return config
}
