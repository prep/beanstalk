package beanstalk

import (
	"log"
	"time"
)

// DefaultOptions returns an Options object with default values.
func DefaultOptions() *Options {
	return &Options{
		QueueSize:        1,
		ReserveTimeout:   time.Second,
		ReconnectTimeout: time.Second * 3,
	}
}

// Options define the configurable parts of the Client, Consumers and Producers.
type Options struct {
	// QueueSize defines how many reserved jobs a single Consumer object is
	// allowed to maintain.
	QueueSize int

	// ReserveTimeout is the maximum amount of time (in seconds) a reserve call
	// is allowed to block.
	ReserveTimeout time.Duration

	// ReconnectTimeout is the time to wait between reconnect attempts.
	ReconnectTimeout time.Duration

	// ReadWriteTimeout is the time a read or write operation on the beanstalk
	// socket is given before it should unblock.
	ReadWriteTimeout time.Duration

	// LogPrefix is a string that gets prepending to every line that is written
	// to the loggers, which suits a special use case by the author. Most people
	// will probably want to use the prefix parameter of log.New().
	LogPrefix string

	// status updates and the like.
	InfoLog *log.Logger

	// ErrorLog is an optional logger for error messages, like read/write errors
	// and unexpected responses.
	ErrorLog *log.Logger
}

// LogInfo writes a log message to the InfoLog logger, if it was set.
func (options *Options) LogInfo(format string, v ...interface{}) {
	if options.InfoLog == nil {
		return
	}

	if options.LogPrefix == "" {
		options.InfoLog.Printf(format, v...)
	} else {
		options.InfoLog.Printf(options.LogPrefix+" "+format, v...)
	}
}

// LogError writes a log message to the ErrorLog logger, if it was set.
func (options *Options) LogError(format string, v ...interface{}) {
	if options.ErrorLog == nil {
		return
	}

	if options.LogPrefix == "" {
		options.ErrorLog.Printf(format, v...)
	} else {
		options.ErrorLog.Printf(options.LogPrefix+" "+format, v...)
	}
}
