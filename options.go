package beanstalk

import "time"

// Options define the configurable parts of the Client, Consumers and Producers.
type Options struct {
	ReserveTimeout   time.Duration
	ReconnectTimeout time.Duration // The time to wait until the next reconnect
	ReadWriteTimeout time.Duration // The time to give request/response combo
}

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
