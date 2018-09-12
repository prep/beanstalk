package beanstalk

import (
	"io"
	"time"
)

type ioHandler interface {
	setupConnection(conn *Conn, config Config) error
	handleIO(conn *Conn, config Config) error
}

func keepConnected(handler ioHandler, conn *Conn, config Config) chan struct{} {
	URI := conn.URI
	close := make(chan struct{})

	go func() {
		for {
			// Run the IO handler.
			if err := handler.handleIO(conn, config); err != nil {
				if err == io.EOF {
					config.logInfo("Disconnected from beanstalk server %s", conn)
				} else {
					config.logError("Disconnected from beanstalk server %s: %s", conn, err)
				}

				return
			}

			select {
			case <-close:
				return
			default:
			}

			// Reconnect to the beanstalk server.
			for {
				var err error
				if conn, err = Dial(URI, config); err != nil {
					config.logError("Unable to connect to beanstalk server %s: %s", URI, err)

					select {
					case <-time.After(config.ReconnectTimeout):
						continue
					case <-close:
						return
					}
				}

				config.logInfo("Connected to beanstalk server %s", conn)

				if err := handler.setupConnection(conn, config); err != nil {
					config.logError("Unable to configure beanstalk connection: %s", err)
					conn.Close()
					continue
				}
			}
		}
	}()

	return close
}

func contains(a []string, s string) bool {
	for _, e := range a {
		if e == s {
			return true
		}
	}

	return false
}
