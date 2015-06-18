package beanstalk

import (
	"net"
	"time"
)

// Connect tries to create a new connection to the specified socket. It returns
// a channel on which a successful connect is advertised, as well as a channel
// to abort a connection attempt in progress.
func Connect(socket string, options *Options) (<-chan net.Conn, chan<- struct{}) {
	newConnection, abortConnect := make(chan net.Conn), make(chan struct{}, 1)

	go func() {
		var newC chan net.Conn

		retry := time.NewTimer(time.Second)
		retry.Stop()

		for {
			conn, err := net.Dial("tcp", socket)
			if conn != nil {
				newC = newConnection
				options.LogInfo("New beanstalk connection to %s (local=%s)", conn.RemoteAddr().String(), conn.LocalAddr().String())
			} else if err != nil {
				retry.Reset(options.ReconnectTimeout)
			}

			select {
			// On successful, offer up the connection.
			case newC <- conn:
				return

			// On failure, retry again after a period of time.
			case <-retry.C:

			// Abort this goroutine when request to do so.
			case <-abortConnect:
				retry.Stop()
				if conn != nil {
					conn.Close()
				}

				return
			}
		}
	}()

	return newConnection, abortConnect
}
