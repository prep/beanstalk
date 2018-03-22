package beanstalk

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"
)

// connect tries to create a new connection to the specified URL. It returns
// a channel on which a successful connect is advertised, as well as a channel
// to abort a connection attempt in progress.
func connect(URL string, options *Options) (<-chan net.Conn, chan<- struct{}) {
	newConnection, abortConnect := make(chan net.Conn), make(chan struct{}, 1)

	go func(URL string, options *Options) {
		var offerC chan net.Conn
		var retry = time.NewTimer(time.Second)
		retry.Stop()

		// Try to establish a connection to the remote beanstalk server.
		for {
			conn, err := dial(URL, options)
			if err != nil {
				retry.Reset(options.ReconnectTimeout)
				options.LogError("Beanstalk connection failed to %s: %s", URL, err)
			} else {
				offerC = newConnection
				options.LogInfo("Beanstalk connection successful to %s (%s)", URL, conn.LocalAddr().String())
			}

			select {
			case <-retry.C:
			case offerC <- conn:
				return
			case <-abortConnect:
				if conn != nil {
					conn.Close()
				}

				retry.Stop()
				return
			}
		}
	}(URL, options)

	return newConnection, abortConnect
}

// dial tries to set up either a non-TLS or a TLS connection to the host:port
// combo specified in socket.
func dial(URL string, options *Options) (net.Conn, error) {
	socket, useTLS, err := ParseURL(URL)
	if err != nil {
		return nil, err
	}

	if !useTLS {
		return net.Dial("tcp", socket)
	}

	conn, err := tls.Dial("tcp", socket, &tls.Config{})
	if conn != nil {
		if err = conn.Handshake(); err == nil {
			return conn, nil
		}
	}

	return nil, err
}

// ParseURL takes a beanstalk URL and returns its hostname:port combination
// and if it's a TLS socket or not.
// Allowable schemes are beanstalk://, beanstalks:// and tls://.
func ParseURL(u string) (socket string, useTLS bool, err error) {
	URL, err := url.Parse(u)
	if err != nil {
		return "", false, err
	}

	socket = URL.Host
	if _, _, err := net.SplitHostPort(socket); err != nil {
		if addrErr, ok := err.(*net.AddrError); ok && addrErr.Err == "missing port in address" {
			socket = net.JoinHostPort(URL.Host, "11300")
		} else {
			return "", false, err
		}
	}

	switch URL.Scheme {
	case "beanstalk":
		return socket, false, nil
	case "beanstalks", "tls":
		return socket, true, nil
	}

	return "", false, fmt.Errorf("%s: unknown scheme for beanstalk URL", URL.Scheme)
}
