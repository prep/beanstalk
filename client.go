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

// Client is responsible for keeping up a client connection to a beanstalk
// server. It advertises newly made connections on the _connCreatedC_ channel
// and the closing of existing connections on the _connClosedC_ channel.
type Client struct {
	socket          string
	options         *Options
	connCreatedC    chan net.Conn
	connClosedC     chan struct{}
	abortNewConnect chan struct{}

	// The state variables.
	c            net.Conn // Only used for Set*Deadline()
	conn         *textproto.Conn
	isConnecting bool
	isConnected  bool
}

// NewClient creates a new beanstalk Client.
func NewClient(socket string, options *Options) Client {
	return Client{
		socket:          socket,
		options:         SanitizedOptions(options),
		connCreatedC:    make(chan net.Conn),
		connClosedC:     make(chan struct{}, 1),
		abortNewConnect: make(chan struct{}, 1)}
}

// OpenConnection creates a new connection to the beanstalk server, unless an
// attempt is already in progress. If a connection is already active, close it
// in favor of setting up a new one.
// A successful connection is reported via the _connCreatedC_ channel.
func (client *Client) OpenConnection() {
	if client.isConnecting {
		return
	}

	client.CloseConnection()
	client.isConnecting = true

	go func() {
		var connCreatedC chan net.Conn

		timeout := time.NewTimer(time.Second)
		timeout.Stop()

		for {
			conn, err := net.Dial("tcp", client.socket)
			if err == nil {
				connCreatedC = client.connCreatedC
			} else {
				timeout.Reset(client.options.ReconnectTimeout)
			}

			select {
			case connCreatedC <- conn:
				return
			case <-timeout.C:
			case <-client.abortNewConnect:
				if conn != nil {
					conn.Close()
				}

				timeout.Stop()
				return
			}
		}
	}()
}

// CloseConnection closes the current connection. If an attempt to set up a
// new connection in the background is happening, abort that.
// A successful closure is reported via the _connClosedC_ channel.
func (client *Client) CloseConnection() {
	if client.conn != nil {
		client.conn.Close()
		client.c, client.conn = nil, nil

		// Clear any previously sent close signal.
		select {
		case <-client.connClosedC:
		default:
		}

		client.connClosedC <- struct{}{}
	}

	client.isConnected = false

	if client.isConnecting {
		client.abortNewConnect <- struct{}{}
		client.isConnecting = false
	}
}

// SetConnection sets the specified connection as the active one. This should
// be called after receiving a new net.Conn via the _connCreatedC_ channel.
func (client *Client) SetConnection(conn net.Conn) {
	client.isConnecting, client.isConnected = false, true
	client.c, client.conn = conn, textproto.NewConn(conn)
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
		job.PutParams.Priority,
		job.PutParams.Delay/time.Second,
		job.PutParams.TTR/time.Second,
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
		client.c.SetReadDeadline(time.Now().Add(client.options.ReserveTimeout + time.Second))
		defer client.c.SetReadDeadline(time.Time{})
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
	if !client.isConnected {
		return ErrNotConnected
	}

	if client.options.ReadWriteTimeout != 0 {
		client.c.SetWriteDeadline(time.Now().Add(client.options.ReadWriteTimeout))
		defer client.c.SetWriteDeadline(time.Time{})
	}

	if err := client.conn.PrintfLine(format, args...); err != nil {
		client.OpenConnection()
		return ErrNotConnected
	}

	return nil
}

// response reads and parses a response from the beanstalk server.
func (client *Client) response() (uint64, []byte, error) {
	line, err := client.conn.ReadLine()
	if err != nil {
		client.OpenConnection()
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
			if _, err := io.ReadFull(client.conn.R, body); err != nil {
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
		if _, err := io.ReadFull(client.conn.R, body); err != nil {
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

	client.OpenConnection()
	return 0, nil, ErrUnexpectedResp
}

// requestResponse sends a request to the beanstalk server and then parses
// and returns its response.
func (client *Client) requestResponse(format string, args ...interface{}) (uint64, []byte, error) {
	if err := client.request(format, args...); err != nil {
		return 0, nil, err
	}

	if client.options.ReadWriteTimeout != 0 {
		client.c.SetReadDeadline(time.Now().Add(client.options.ReadWriteTimeout))
		defer client.c.SetReadDeadline(time.Time{})
	}

	return client.response()
}
