package beanstalk

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

// These error may be returned by any of Conn's functions.
var (
	ErrBuried       = errors.New("job was buried")
	ErrDeadline     = errors.New("deadline soon")
	ErrDisconnected = errors.New("connection is disconnected")
	ErrNotFound     = errors.New("job not found")
	ErrNotIgnored   = errors.New("tube not ignored")
	ErrUnexpected   = errors.New("unexpected response received")
)

type response struct {
	ID    uint64
	Body  []byte
	Error error
}

// Conn describes a connection to a beanstalk server.
type Conn struct {
	URI      string
	Closed   <-chan error
	closed   chan error
	conn     net.Conn
	text     *textproto.Conn
	config   Config
	respC    chan response
	lastTube string
	mu       sync.Mutex
}

// Dial into a beanstalk server.
func Dial(URI string, config Config) (*Conn, error) {
	URL, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}

	// Determine the protocol scheme of the URI.
	switch strings.ToLower(URL.Scheme) {
	case "beanstalks", "tls":
		URL.Scheme = "tls"
	case "beanstalk":
		URL.Scheme = "beanstalk"
	default:
		return nil, fmt.Errorf("%s: unknown beanstalk URI scheme", URL.Scheme)
	}

	// If no port has been specified, add the appropriate one.
	if _, _, err = net.SplitHostPort(URL.Host); err.Error() == "missing port in address" {
		if URL.Scheme == "tls" {
			URL.Host += ":11400"
		} else {
			URL.Host += ":11300"
		}
	}

	// Dial into the beanstalk server.
	var netConn net.Conn
	if URL.Scheme == "tls" {
		tlsConn, err := tls.Dial("tcp", URL.Host, config.TLSConfig)
		if err != nil {
			return nil, err
		}

		if err = tlsConn.Handshake(); err != nil {
			return nil, err
		}

		netConn = tlsConn
	} else {
		var err error
		if netConn, err = net.Dial("tcp", URL.Host); err != nil {
			return nil, err
		}
	}

	closed := make(chan error, 1)
	conn := &Conn{
		URI:    URL.String(),
		Closed: closed,
		closed: closed,
		conn:   netConn,
		text:   textproto.NewConn(netConn),
		config: config.normalize(),
		respC:  make(chan response),
	}

	go conn.readResponse()
	return conn, nil
}

// Close this connection.
func (conn *Conn) Close() error {
	return conn.text.Close()
}

func (conn *Conn) String() string {
	return conn.URI + " (local=" + conn.conn.LocalAddr().String() + ")"
}

func (conn *Conn) readResponse() {
	for {
		line, err := conn.text.ReadLine()
		if err != nil {
			conn.respC <- response{Error: err}
			close(conn.respC)

			conn.closed <- err
			close(conn.closed)

			conn.text.Close()
			return
		}

		parts := strings.SplitN(line, " ", 3)

		var resp response
		switch parts[0] {
		case "OK":
			if len(parts) != 2 {
				resp.Error = ErrUnexpected
				break
			}

			size, err := strconv.ParseInt(parts[1], 10, 32)
			if err != nil {
				resp.Error = err
				break
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(conn.text.R, body); err != nil {
				resp.Error = err
			}

		case "RESERVED":
			if len(parts) != 3 {
				resp.Error = ErrUnexpected
				break
			}

			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				resp.Error = err
				break
			}
			size, err := strconv.ParseInt(parts[2], 10, 32)
			if err != nil {
				resp.Error = err
				break
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(conn.text.R, body); err != nil {
				resp.Error = err
				break
			}

			resp.ID = id
			resp.Body = body[:size]

		case "NOT_IGNORED":
			resp.Error = ErrNotIgnored

		case "DELETED", "RELEASED", "TIMED_OUT", "TOUCHED", "USING", "WATCHING":
		case "BURIED":
			resp.Error = ErrBuried
		case "DEADLINE_SOON":
			resp.Error = ErrDeadline
		case "NOT_FOUND":
			resp.Error = ErrNotFound
		default:
			resp.Error = ErrUnexpected
		}

		conn.respC <- resp
	}
}

func (conn *Conn) command(ctx context.Context, format string, params ...interface{}) (uint64, []byte, error) {
	select {
	case <-conn.closed:
		return 0, nil, ErrDisconnected
	default:
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.conn.SetDeadline(deadline); err != nil {
			return 0, nil, err
		}

		defer conn.conn.SetDeadline(time.Time{})
	}

	err := conn.text.PrintfLine(format, params...)
	if err != nil {
		return 0, nil, err
	}

	resp, ok := <-conn.respC
	if !ok {
		return 0, nil, ErrDisconnected
	}
	if resp.Error != nil {
		return 0, nil, resp.Error
	}

	return resp.ID, resp.Body, nil
}

func (conn *Conn) lcommand(ctx context.Context, format string, params ...interface{}) (uint64, []byte, error) {
	conn.mu.Lock()
	id, body, err := conn.command(ctx, format, params...)
	conn.mu.Unlock()

	return id, body, err
}

func (conn *Conn) bury(ctx context.Context, job *Job, priority uint32) error {
	_, _, err := conn.lcommand(ctx, "bury %d %d", job.ID, priority)
	if err == ErrBuried {
		return nil
	}

	return err
}

func (conn *Conn) delete(ctx context.Context, job *Job) error {
	_, _, err := conn.lcommand(ctx, "delete %d", job.ID)
	return err
}

// Ignore the specified tube.
func (conn *Conn) Ignore(ctx context.Context, tube string) error {
	_, _, err := conn.lcommand(ctx, "ignore %s", tube)
	return err
}

// Put a job in the specified tube.
func (conn *Conn) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if tube != conn.lastTube {
		if _, _, err := conn.command(ctx, "use %s", tube); err != nil {
			return 0, err
		}

		conn.lastTube = tube
	}

	id, _, err := conn.command(ctx, "put %d %d %d %d\r\n%s", params.Priority, params.Delay/time.Second, params.TTR/time.Second, len(body), body)
	return id, err
}

func (conn *Conn) release(ctx context.Context, job *Job, priority uint32, delay time.Duration) error {
	_, _, err := conn.lcommand(ctx, "release %d %d %d", job.ID, priority, delay/time.Second)
	return err
}

// ReserveWithTimeout tries to reserve a job and block for up to a maximum of
// timeout. If no job could be reserved, this function will return without a
// job or error.
func (conn *Conn) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (*Job, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	id, body, err := conn.command(ctx, "reserve-with-timeout %d", timeout/time.Second)
	if err != nil {
		if err == ErrDeadline {
			return nil, nil
		}

		return nil, err
	}

	job := &Job{ID: id, Body: body, ReservedAt: time.Now(), conn: conn}

	// If this command errors out, it's either a NOT_FOUND response or an error
	// on the connection. If it's the former, the TTR was probably very short and
	// the connection very slow.
	// Either way, the job that was reserved is already lost.
	if _, body, err = conn.command(ctx, "stats-job %d", job.ID); err != nil {
		if err == ErrNotFound {
			return nil, nil
		}

		return nil, err
	}

	// If the job stats are unmarshallable, return the error and expect the caller
	// to close the connection which takes care of the job's reservation.
	// However, in case the caller doesn't and still wants the job, return it anyway.
	if err := yaml.Unmarshal(body, &job.Stats); err != nil {
		return job, err
	}

	job.Stats.Age *= time.Second
	job.Stats.Delay *= time.Second
	job.Stats.TTR *= time.Second
	job.Stats.TimeLeft *= time.Second

	return job, nil
}

// touch the job thereby resetting its reserved status.
func (conn *Conn) touch(ctx context.Context, job *Job) error {
	if _, _, err := conn.lcommand(ctx, "touch %d", job.ID); err != nil {
		return err
	}

	job.ReservedAt = time.Now()
	return nil
}

// Watch the specified tube.
func (conn *Conn) Watch(ctx context.Context, tube string) error {
	_, _, err := conn.lcommand(ctx, "watch %s", tube)
	return err
}
