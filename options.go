package beanstalk

import (
	"log"
	"time"
)

// DefaultOptions returns an Options object with default values.
func DefaultOptions() Options {
	return Options{
		ReserveTimeout:   time.Second,
		ReconnectTimeout: time.Second * 3,
	}
}

// SanitizeOptions returns sane Options structure to work with.
func SanitizeOptions(options Options) Options {
	if options.ReserveTimeout < time.Second {
		options.ReserveTimeout = time.Second
	}

	if options.ReconnectTimeout < time.Second {
		options.ReconnectTimeout = time.Second
	}

	if options.ReadWriteTimeout != 0 && options.ReadWriteTimeout < time.Millisecond {
		options.ReadWriteTimeout = time.Millisecond
	}

	return options
}

// Options define the configurable parts of the Client, Consumers and Producers.
type Options struct {
	ReserveTimeout   time.Duration
	ReconnectTimeout time.Duration // The time to wait until the next reconnect
	ReadWriteTimeout time.Duration // The time to give request/response combo

	LogPrefix string      // A prefix to every line being written to the loggers
	InfoLog   *log.Logger // The logger for normal information
	ErrorLog  *log.Logger // The logger for errors
}

// LogInfo writes a log message to the InfoLog logger, if it was set.
func (options Options) LogInfo(format string, v ...interface{}) {
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
func (options Options) LogError(format string, v ...interface{}) {
	if options.ErrorLog == nil {
		return
	}

	if options.LogPrefix == "" {
		options.ErrorLog.Printf(format, v...)
	} else {
		options.ErrorLog.Printf(options.LogPrefix+" "+format, v...)
	}
}
