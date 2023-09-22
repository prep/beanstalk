package beanstalk

import (
	"crypto/tls"
	"time"
)

// Config is used to configure a Consumer, Producer or Conn.
type Config struct {
	// Multiply the list of URIs to create a larger pool of connections.
	//
	// The default is to have 1.
	Multiply int
	// NumGoroutines is the number of goroutines that the Receive method will
	// spin up to process jobs concurrently.
	//
	// The default is to spin up 10 goroutines.
	NumGoroutines int
	// ConnTimeout configures the read and write timeout of the connection. This
	// can be overridden by a context deadline if its value is lower.
	//
	// Note that this does not work with Reserve() and might interfere with
	// ReserveWithTimeout() if configured incorrectly.
	//
	// The default is to have no timeout.
	ConnTimeout time.Duration
	// ReserveTimeout is the time a consumer connection waits between reserve
	// attempts if the last attempt failed to reserve a job.
	//
	// The default is to wait 1 seconds.
	ReserveTimeout time.Duration
	// ReconnectTimeout is the timeout between reconnects.
	//
	// The default is to wait 3 seconds.
	ReconnectTimeout time.Duration
	// TLSConfig describes the configuration that is used when Dial() makes a
	// TLS connection.
	TLSConfig *tls.Config
	// InfoFunc is called to log informational messages.
	InfoFunc func(message string)
	// ErrorFunc is called to log error messages.
	ErrorFunc func(err error, message string)
	// IgnoreURIValidation skips URI validation. This is useful in dynamic
	// infrastructure (like k8s) where the DNS name of the beanstalk server
	// might not resolve at startup time.
	IgnoreURIValidation bool
}

func (config Config) normalize() Config {
	if config.Multiply < 1 {
		config.Multiply = 1
	}
	if config.NumGoroutines < 1 {
		config.NumGoroutines = 10
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
