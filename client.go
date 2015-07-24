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
	ErrBuried           = errors.New("Job is buried")
	ErrConnectionClosed = errors.New("Remote end closed connection")
	ErrDeadlineSoon     = errors.New("Deadline soon")
	ErrDraining         = errors.New("Server in draining mode")
	ErrExpectedCRLF     = errors.New("Expected CRLF after job body")
	ErrJobTooBig        = errors.New("Job body too big")
	ErrNotConnected     = errors.New("Not connected")
	ErrNotFound         = errors.New("Job not found")
	ErrNotIgnored       = errors.New("Tube cannot be ignored")
	ErrOutOfMemory      = errors.New("Server is out of memory")
	ErrUnexpectedResp   = errors.New("Unexpected response from server")
)

// Client implements a simple beanstalk API.
type Client struct {
	options     *Options
	conn        net.Conn
	textConn    *textproto.Conn
	isConnected bool
}

// NewClient returns a new beanstalk Client object.
func NewClient(conn net.Conn, options *Options) *Client {
	if options == nil {
		options = DefaultOptions()
	}

	return &Client{
		options:     options,
		conn:        conn,
		textConn:    textproto.NewConn(conn),
		isConnected: true}
}

// Close the connection to the beanstalk server.
func (client *Client) Close() {
	if client.textConn == nil {
		return
	}

	client.options.LogInfo("Closing connection to beanstalk server %s (local=%s)", client.conn.RemoteAddr().String(), client.conn.LocalAddr().String())
	client.textConn.Close()
	client.textConn = nil
	client.conn = nil
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
func (client *Client) Put(putRequest *PutRequest) (uint64, error) {
	id, _, err := client.requestResponse("put %d %d %d %d\r\n%s",
		putRequest.Params.Priority,
		putRequest.Params.Delay/time.Second,
		putRequest.Params.TTR/time.Second,
		len(putRequest.Body),
		putRequest.Body)

	return id, err
}

// Release a reserved job. This is done after being unable to process the job,
// but another consumer might be successful.
func (client *Client) Release(job *Job, priority uint32, delay time.Duration) error {
	_, _, err := client.requestResponse("release %d %d %d", job.ID, priority, delay/time.Second)
	return err
}

// Reserve retrieves a new job.
func (client *Client) Reserve(timeout time.Duration) (*Job, error) {
	err := client.request("reserve-with-timeout %d", timeout/time.Second)
	if err != nil {
		return nil, err
	}

	// Set a read deadline that is slightly longer than the reserve timeout.
	if timeout != 0 {
		client.conn.SetReadDeadline(time.Now().Add(timeout + time.Second))
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
	if client.options.ReadWriteTimeout != 0 {
		client.conn.SetWriteDeadline(time.Now().Add(client.options.ReadWriteTimeout))
		defer client.conn.SetWriteDeadline(time.Time{})
	}

	if err := client.textConn.PrintfLine(format, args...); err != nil {
		if err == io.EOF {
			return ErrConnectionClosed
		}
		return err
	}

	return nil
}

// response reads and parses a response from the beanstalk server.
func (client *Client) response() (uint64, []byte, error) {
	line, err := client.textConn.ReadLine()
	if err != nil {
		if err == io.EOF {
			return 0, nil, ErrConnectionClosed
		}
		return 0, nil, err
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

	// Deadline soon means a reserved job is about to expire.
	case "DEADLINE_SOON":
		return 0, nil, ErrDeadlineSoon

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
