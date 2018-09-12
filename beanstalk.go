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
		var err error
		for {
			// Reconnect to the beanstalk server.
			for conn == nil {
				if conn, err = Dial(URI, config); err != nil {
					config.ErrorLog.Printf("Unable to connect to beanstalk server %s: %s", URI, err)

					select {
					// Wait a bit and try again.
					case <-time.After(config.ReconnectTimeout):
						continue
					case <-close:
						return
					}
				}
			}

			config.InfoLog.Printf("Connected to beanstalk server %s", conn)

			// Set up the connection. If not successful, close the connection, wait
			// a bit and reconnect.
			err := handler.setupConnection(conn, config)
			if err != nil {
				config.InfoLog.Printf("Unable to set up the beanstalk connection: %s", err)
				conn.Close()
				conn = nil

				select {
				case <-time.After(config.ReconnectTimeout):
				case <-close:
					return
				}

				continue
			}

			// Run the IO handler.
			if err = handler.handleIO(conn, config); err != nil {
				if err == io.EOF {
					config.InfoLog.Printf("Disconnected from beanstalk server %s", conn)
				} else {
					config.ErrorLog.Printf("Disconnected from beanstalk server %s: %s", conn, err)
				}
			}

			conn.Close()
			conn = nil

			select {
			case <-close:
				return
			default:
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
