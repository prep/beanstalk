package beanstalk

import (
	"errors"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// Errors that can be returned by the beanstalk client functions.
var (
	ErrBuried         = errors.New("Job is buried")
	ErrDraining       = errors.New("Server in draining mode")
	ErrExpectedCRLF   = errors.New("Expected CRLF after job body")
	ErrJobTooBig      = errors.New("Job body too big")
	ErrNotConnected   = errors.New("Not connected")
	ErrNotFound       = errors.New("Job not found")
	ErrNotIgnored     = errors.New("Tube cannot be ignored")
	ErrOutOfMemory    = errors.New("Server is out of memory")
	ErrUnexpectedResp = errors.New("Unexpected response from server")
)

// Client implements a simple beanstalk API that is responsible for maintaining
// a connection to a beanstalk server. Connection status changes are advertised
// on the Connected channel.
type Client struct {
	// Connected updates the implementer about the status of the connection that
	// this client maintains.
	Connected <-chan bool

	connected      chan bool     // A writeable version of Connected.
	newConn        chan net.Conn // Advertise a successful reconnect.
	abortReconnect chan struct{} // Abort a reconnect in progress.
	isConnecting   bool          // True if a reconnect is in progress.

	socket  string
	options *Options

	// These connection objects point to the same filedescriptor, but conn is
	// used exclusively for the set*Deadline() calls and textConn for beanstalk
	// protocol parsing.
	conn     net.Conn
	textConn *textproto.Conn
}

// NewClient returns a new beanstalk Client object.
func NewClient(socket string, options *Options) *Client {
	connected := make(chan bool, 1)

	client := &Client{
		Connected:      connected,
		connected:      connected,
		newConn:        make(chan net.Conn),
		abortReconnect: make(chan struct{}, 1),
		socket:         socket,
		options:        SanitizedOptions(options)}
	client.reconnect()

	return client
}

// reconnect to the beanstalk server.
func (client *Client) reconnect() {
	if client.isConnecting {
		return
	}

	client.Close()
	client.isConnecting = true

	go func() {
		retry := time.NewTimer(time.Second)
		retry.Stop()

		for {
			if conn, err := net.Dial("tcp", client.socket); err == nil {
				client.connected <- true

				// Offer up the new connection. If an abort comes in, close the new
				// connection and clear out the Connected channel.
				select {
				case client.newConn <- conn:
				case <-client.abortReconnect:
					conn.Close()

					select {
					case <-client.Connected:
					default:
					}
				}

				return
			}

			retry.Reset(client.options.ReconnectTimeout)

			select {
			case <-retry.C:
			case <-client.abortReconnect:
				retry.Stop()
				return
			}
		}
	}()
}

// Close the existing connection this client might have to a beanstalk server
// and abort any reconnect in progress.
func (client *Client) Close() {
	if client.isConnecting {
		client.abortReconnect <- struct{}{}
		client.isConnecting = false
	}

	if client.conn != nil {
		client.conn.Close()
		client.conn, client.textConn = nil, nil
		client.connected <- false
	}
}

// Bury a reserved job. This is done after being unable to process the job and
// it is likely that other consumers won't either.
func (client *Client) Bury(job *Job, priority uint32) error {
	_, _, err := client.requestResponse("bury %d %d", job.ID, priority)
	if err == ErrBuried {
		return nil
	}

	return err
}

// Delete a reserved job. This is done after successful processing.
func (client *Client) Delete(job *Job) error {
	_, _, err := client.requestResponse("delete %d", job.ID)
	return err
}

// Ignore removes an active tube from the watch list.
func (client *Client) Ignore(tube string) error {
	_, _, err := client.requestResponse("ignore %s", tube)
	return err
}

// Put a new job into beanstalk.
func (client *Client) Put(job *Put) (uint64, error) {
	id, _, err := client.requestResponse("put %d %d %d %d\r\n%s",
		job.Params.Priority,
		job.Params.Delay/time.Second,
		job.Params.TTR/time.Second,
		len(job.Body),
		job.Body)

	return id, err
}

// Release a reserved job. This is done after being unable to process the job,
// but another consumer might be successful.
func (client *Client) Release(job *Job, priority uint32, delay time.Duration) error {
	_, _, err := client.requestResponse("release %d %d %d", job.ID, priority, delay/time.Second)
	return err
}

// Reserve retrieves a new job.
func (client *Client) Reserve() (*Job, error) {
	err := client.request("reserve-with-timeout %d", client.options.ReserveTimeout/time.Second)
	if err != nil {
		return nil, err
	}

	// Set a read deadline that is slightly longer than the reserve timeout.
	if client.options.ReserveTimeout != 0 {
		client.conn.SetReadDeadline(time.Now().Add(client.options.ReserveTimeout + time.Second))
		defer client.conn.SetReadDeadline(time.Time{})
	}

	job := &Job{TTR: time.Second}
	job.ID, job.Body, err = client.response()
	if err != nil {
		return nil, err
	}
	if job.ID == 0 {
		return nil, nil
	}

	// Fetch the TTR value for this job via stats-job. If this fails, ignore it.
	if _, yaml, err := client.requestResponse("stats-job %d", job.ID); err == nil {
		if val, err := yamlValue(yaml, "pri"); err == nil {
			if prio, err := strconv.ParseUint(val, 10, 32); err == nil {
				job.Priority = uint32(prio)
			}
		}

		if val, err := yamlValue(yaml, "ttr"); err == nil {
			if ttr, err := strconv.Atoi(val); err == nil {
				job.TTR = time.Duration(ttr) * time.Second
			}
		}
	}

	// Shave off 100 milliseconds to have some room to play with between touches.
	job.TTR -= 100 * time.Millisecond

	return job, nil
}

// Touch a job to extend the TTR of the reserved job.
func (client *Client) Touch(job *Job) error {
	_, _, err := client.requestResponse("touch %d", job.ID)
	return err
}

// Use the specified tube for the upcoming put requests.
func (client *Client) Use(tube string) error {
	_, _, err := client.requestResponse("use %s", tube)
	return err
}

// Watch adds a tube to the watch list.
func (client *Client) Watch(tube string) error {
	_, _, err := client.requestResponse("watch %s", tube)
	return err
}

// request sends a request to the beanstalk server.
func (client *Client) request(format string, args ...interface{}) error {
	// If there is no active connection, check to see if the reconnect()
	// goroutine is offering up a new one.
	if client.conn == nil {
		select {
		case client.conn = <-client.newConn:
			client.isConnecting = false
			client.textConn = textproto.NewConn(client.conn)
		default:
			return ErrNotConnected
		}
	}

	if client.options.ReadWriteTimeout != 0 {
		client.conn.SetWriteDeadline(time.Now().Add(client.options.ReadWriteTimeout))
		defer client.conn.SetWriteDeadline(time.Time{})
	}

	if err := client.textConn.PrintfLine(format, args...); err != nil {
		client.reconnect()
		return ErrNotConnected
	}

	return nil
}

// response reads and parses a response from the beanstalk server.
func (client *Client) response() (uint64, []byte, error) {
	line, err := client.textConn.ReadLine()
	if err != nil {
		client.reconnect()
		return 0, nil, ErrNotConnected
	}

	items := strings.SplitN(line, " ", 2)
	if len(items[0]) == 0 {
		return 0, nil, ErrUnexpectedResp
	}

	var response, rest = items[0], ""
	if len(items) == 2 {
		rest = items[1]
	}

	switch response {
	// Simple successful responses.
	case "DELETED", "RELEASED", "TIMED_OUT", "TOUCHED", "USING", "WATCHING":
		return 0, nil, nil

	// BURIED can either be a successful response to a _bury_ command or an
	// unsuccessful response to the _put_ and _release_ commands.
	case "BURIED":
		if rest != "" {
			// The response to the _put_ command provides an id of the job.
			if id, err := strconv.ParseUint(rest, 10, 64); err == nil {
				return id, nil, ErrBuried
			}
		}

		return 0, nil, ErrBuried

	// INSERTED is a successful response to a _put_ command.
	case "INSERTED":
		if id, err := strconv.ParseUint(rest, 10, 64); err == nil {
			return id, nil, nil
		}

	// OK is a successful response to a request that responds with YAML data.
	case "OK":
		if size, err := strconv.Atoi(rest); err == nil {
			body := make([]byte, size+2)
			if _, err := io.ReadFull(client.textConn.R, body); err != nil {
				break
			}

			return 0, body[:size], nil
		}

	// A RESERVED response is a successful response to a _reserve_ command.
	case "RESERVED":
		resInfo := strings.SplitN(rest, " ", 2)
		if len(resInfo) != 2 {
			break
		}

		id, err := strconv.ParseUint(resInfo[0], 10, 64)
		if err != nil {
			break
		}
		size, err := strconv.Atoi(resInfo[1])
		if err != nil {
			break
		}

		body := make([]byte, size+2)
		if _, err := io.ReadFull(client.textConn.R, body); err != nil {
			break
		}

		return id, body[:size], nil

	// NOT_FOUND is a response to an unsuccessful _bury_, _delete_, _touch_ or
	// _release_ command.
	case "NOT_FOUND":
		return 0, nil, ErrNotFound

	// NOT_IGNORED is a response to an unsuccessful _ignore_ command.
	case "NOT_IGNORED":
		return 0, nil, ErrNotIgnored

	// The following responses can occur after an unsuccessful _put_ command.
	case "DRAINING":
		return 0, nil, ErrDraining
	case "EXPECTED_CRLF":
		return 0, nil, ErrExpectedCRLF
	case "JOB_TOO_BIG":
		return 0, nil, ErrJobTooBig
	case "OUT_OF_MEMORY":
		return 0, nil, ErrOutOfMemory
	}

	client.reconnect()
	return 0, nil, ErrUnexpectedResp
}

// requestResponse sends a request to the beanstalk server and then parses
// and returns its response.
func (client *Client) requestResponse(format string, args ...interface{}) (uint64, []byte, error) {
	if err := client.request(format, args...); err != nil {
		return 0, nil, err
	}

	if client.options.ReadWriteTimeout != 0 {
		client.conn.SetReadDeadline(time.Now().Add(client.options.ReadWriteTimeout))
		defer client.conn.SetReadDeadline(time.Time{})
	}

	return client.response()
}
