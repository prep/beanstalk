package beanstalk

import (
	"crypto/tls"
	"io/ioutil"
	"log"
	"time"
)

// A Config structure is used to configure a Consumer, Producer, one of its
// pools or Conn.
type Config struct {
	// NumGoroutines is the number of goroutines that the Receive() method will
	// spin up.
	// The default is to spin up 1 goroutine.
	NumGoroutines int
	// ReserveTimeout is the time a consumer should wait before reserving a job,
	// when the last attempt didn't yield a job.
	// The default is to wait 5 seconds.
	ReserveTimeout time.Duration
	// ReleaseTimeout is the time a consumer should hold a reserved job before
	// it is released back.
	// The default is to wait 3 seconds.
	ReleaseTimeout time.Duration
	// ReconnectTimeout is the timeout between reconnects.
	// The default is to wait 10 seconds.
	ReconnectTimeout time.Duration
	// TLSConfig describes the configuration that is used when Dial() makes a
	// TLS connection.
	TLSConfig *tls.Config
	// InfoLog is used to log informational messages.
	InfoLog *log.Logger
	// ErrorLog is used to log error messages.
	ErrorLog *log.Logger

	jobC chan *Job
}

func (config Config) normalize() Config {
	if config.NumGoroutines < 1 {
		config.NumGoroutines = 1
	}
	if config.ReserveTimeout <= 0 {
		config.ReserveTimeout = 5 * time.Second
	}
	if config.ReleaseTimeout <= 0 {
		config.ReleaseTimeout = 3 * time.Second
	}
	if config.ReconnectTimeout <= 0 {
		config.ReconnectTimeout = 10 * time.Second
	}

	if config.InfoLog == nil {
		config.InfoLog = log.New(ioutil.Discard, "", 0)
	}
	if config.ErrorLog == nil {
		config.ErrorLog = log.New(ioutil.Discard, "", 0)
	}

	if config.jobC == nil {
		config.jobC = make(chan *Job)
	}

	return config
}
