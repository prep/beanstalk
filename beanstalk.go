package beanstalk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type uriType string

const (
	uriTCPType uriType = "tcp"
	uriTLSType uriType = "tls"
	uriUDSType uriType = "unix"
)

// parseURI returns the socket of the specified URI and if the connection is
// supposed to be a TLS or plaintext connection. Valid URI schemes are:
//
//		beanstalk://host:port
//		beanstalks://host:port
//		tls://host:port
//              unix://path
//
// Where both the beanstalks and tls scheme mean the same thing. Alternatively,
// it is also possibly to just specify the host:port combo which is assumed to
// be a plaintext connection.
func parseURI(uri string) (string, uriType, error) {
	address := uri
	uriType := uriTCPType

	if strings.Contains(uri, "://") {
		url, err := url.Parse(uri)
		if err != nil {
			return "", uriType, err
		}

		// Determine the protocol scheme of the URI.
		switch strings.ToLower(url.Scheme) {
		case "beanstalk":
			uriType = uriTCPType
			address = url.Host
		case "beanstalks", "tls":
			uriType = uriTLSType
			address = url.Host
		case "unix":
			uriType = uriUDSType
			address = url.Path
		default:
			return "", uriType, fmt.Errorf("%s: unknown beanstalk URI scheme", url.Scheme)
		}

	}

	if uriType == uriUDSType {
		return address, uriType, nil
	}

	// Validate the resulting host:port combo.
	_, _, err := net.SplitHostPort(address)
	switch {
	case err != nil && strings.Contains(err.Error(), "missing port in address"):
		if uriType == uriTLSType {
			address += ":11400"
		} else {
			address += ":11300"
		}
	case err != nil:
		return "", uriType, err
	}

	return address, uriType, nil
}

// includes returns true if s is contained in a.
func includes(a []string, s string) bool {
	for _, e := range a {
		if e == s {
			return true
		}
	}

	return false
}

// ValidURIs returns an error if any of the specified URIs is invalid, or if
// the host names in the URIs could not be resolved.
func ValidURIs(uris []string) error {
	if len(uris) == 0 {
		return errors.New("no URIs specified")
	}

	for _, uri := range uris {
		address, uriType, err := parseURI(uri)
		if err != nil {
			return err
		}

		if uriType == uriUDSType {
			continue
		}

		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}

		if _, err = net.LookupHost(host); err != nil {
			return err
		}
	}

	return nil
}

type connHandler struct {
	// setup the connection after it has been established. This is used by
	// the consumer to watch the proper tubes.
	setup func(context.Context, *Conn) error
	// handle the connection after the setup has been done. This method returns
	// on connection error or when the context is cancelled.
	handle func(context.Context, *Conn) error
}

// maintainConn is responsible for maintaining a connection to a beanstalk
// server on behalf of a Consumer or Producer.
func maintainConn(ctx context.Context, uri string, config Config, handler connHandler) {
	var conn *Conn
	var err error

	for {
		// Create a connection to the beanstalk server.
		if conn, err = Dial(uri, config); err != nil {
			config.ErrorFunc(err, fmt.Sprintf("Unable to connect to beanstalk server: %s", uri))

			select {
			case <-time.After(config.ReconnectTimeout):
				continue
			case <-ctx.Done():
				return
			}
		}

		config.InfoFunc(fmt.Sprintf("Connected to beanstalk server %s", conn))

		// Set up the connection before really using it.
		if handler.setup != nil {
			if err = handler.setup(ctx, conn); err != nil {
				config.ErrorFunc(err, "Unable to set up the beanstalk connection")

				_ = conn.Close()
				select {
				case <-time.After(config.ReconnectTimeout):
					continue
				case <-ctx.Done():
					return
				}
			}
		}

		// Hand over the connection.
		if err = handler.handle(ctx, conn); err != nil && !errors.Is(err, ErrDisconnected) {
			config.ErrorFunc(err, fmt.Sprintf("Disconnected from beanstalk server %s", conn))
		} else {
			config.InfoFunc(fmt.Sprintf("Disconnected from beanstalk server %s", conn))
		}

		_ = conn.Close()
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// multiply a slice by the specified amount. This is used to multiply the
// number of TCP client connections.
func multiply(list []string, multiply int) []string {
	var results []string
	for i := 0; i < multiply; i++ {
		results = append(results, list...)
	}

	return results
}
