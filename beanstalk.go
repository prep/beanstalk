package beanstalk

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// ParseURI returns the socket of the specified URI and if the connection is
// supposed to be a TLS or plaintext connection. Valid URI schemes are:
//
//		beanstalk://host:port
//		beanstalks://host:port
//		tls://host:port
//
// Where both the beanstalks and tls scheme mean the same thing. Alternatively,
// it is also possibly to just specify the host:port combo which is assumed to
// be a plaintext connection.
func ParseURI(uri string) (string, bool, error) {
	var host string
	var isTLS bool

	if strings.Contains(uri, "://") {
		url, err := url.Parse(uri)
		if err != nil {
			return "", false, err
		}

		// Determine the protocol scheme of the URI.
		switch strings.ToLower(url.Scheme) {
		case "beanstalk":
		case "beanstalks", "tls":
			isTLS = true
		default:
			return "", false, fmt.Errorf("%s: unknown beanstalk URI scheme", url.Scheme)
		}

		host = url.Host
	} else {
		host = uri
	}

	// Validate the resulting host:port combo.
	_, _, err := net.SplitHostPort(host)
	switch {
	case err != nil && strings.Contains(err.Error(), "missing port in address"):
		if isTLS {
			host += ":11400"
		} else {
			host += ":11300"
		}
	case err != nil:
		return "", false, err
	}

	return host, isTLS, nil
}

func includes(a []string, s string) bool {
	for _, e := range a {
		if e == s {
			return true
		}
	}

	return false
}

func contextTimeoutFunc(d time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return fn(ctx)
}

type ioHandler interface {
	setupConnection(conn *Conn, config Config) error
	handleIO(conn *Conn, config Config) error
}

// keepConnected is responsible for keeping a connection to a URI up.
func keepConnected(handler ioHandler, conn *Conn, config Config, close chan struct{}) {
	URI := conn.URI

	go func() {
		var err error
		for {
			// Reconnect to the beanstalk server if no connection is active.
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
				_ = conn.Close()
				conn = nil

				select {
				case <-time.After(config.ReconnectTimeout):
				case <-close:
					return
				}

				continue
			}

			// call the IO handler for as long as it wants it, or the connection is up.
			if err = handler.handleIO(conn, config); err != nil && err != ErrDisconnected {
				config.ErrorLog.Printf("Disconnected from beanstalk server %s: %s", conn, err)
			} else {
				config.InfoLog.Printf("Disconnected from beanstalk server %s", conn)
			}

			_ = conn.Close()
			conn = nil

			select {
			case <-close:
				return
			default:
			}
		}
	}()
}
