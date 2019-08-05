package beanstalk

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opencensus.io/trace"
	"gopkg.in/yaml.v2"
)

// These error may be returned by any of Conn's methods.
var (
	ErrBuried       = errors.New("job was buried")
	ErrDeadlineSoon = errors.New("deadline soon")
	ErrDisconnected = errors.New("client disconnected")
	ErrNotFound     = errors.New("job not found")
	ErrTimedOut     = errors.New("reserve timed out")
	ErrNotIgnored   = errors.New("tube not ignored")
	ErrTubeTooLong  = errors.New("tube name too long")
	ErrUnexpected   = errors.New("unexpected response received")
)

// Conn describes a connection to a beanstalk server.
type Conn struct {
	URI      string
	config   Config
	conn     net.Conn
	text     *textproto.Conn
	lastTube string
	mu       sync.Mutex
}

// Dial into a beanstalk server.
func Dial(uri string, config Config) (*Conn, error) {
	socket, isTLS, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}

	// Dial into the beanstalk server.
	var netConn net.Conn
	if isTLS {
		tlsConn, err := tls.Dial("tcp", socket, config.TLSConfig)
		if err != nil {
			return nil, err
		}

		if err = tlsConn.Handshake(); err != nil {
			return nil, err
		}

		netConn = tlsConn
	} else {
		var err error
		if netConn, err = net.Dial("tcp", socket); err != nil {
			return nil, err
		}
	}

	return &Conn{
		URI:    uri,
		config: config.normalize(),
		conn:   netConn,
		text:   textproto.NewConn(netConn),
	}, nil
}

// Close this connection.
func (conn *Conn) Close() error {
	return conn.conn.Close()
}

func (conn *Conn) String() string {
	return conn.URI + " (local=" + conn.conn.LocalAddr().String() + ")"
}

func (conn *Conn) command(ctx context.Context, format string, params ...interface{}) (uint64, []byte, error) {
	// Write a command and read the response.
	id, body, err := func() (uint64, []byte, error) {
		if deadline, ok := ctx.Deadline(); ok {
			if err := conn.conn.SetDeadline(deadline); err != nil {
				return 0, nil, err
			}

			defer conn.conn.SetDeadline(time.Time{})
		}

		if err := conn.text.PrintfLine(format, params...); err != nil {
			return 0, nil, err
		}

		line, err := conn.text.ReadLine()
		if err != nil {
			return 0, nil, err
		}

		parts := strings.SplitN(line, " ", 3)
		switch parts[0] {
		case "INSERTED":
			if len(parts) != 2 {
				return 0, nil, ErrUnexpected
			}

			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return 0, nil, ErrUnexpected
			}

			return id, nil, err

		case "OK":
			if len(parts) != 2 {
				return 0, nil, ErrUnexpected
			}

			size, err := strconv.ParseInt(parts[1], 10, 32)
			if err != nil {
				return 0, nil, err
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(conn.text.R, body); err != nil {
				return 0, nil, err
			}

			return 0, body, nil

		case "RESERVED":
			if len(parts) != 3 {
				return 0, nil, ErrUnexpected
			}

			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return 0, nil, err
			}
			size, err := strconv.ParseInt(parts[2], 10, 32)
			if err != nil {
				return 0, nil, err
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(conn.text.R, body); err != nil {
				return 0, nil, err
			}

			return id, body[:size], nil

		case "DELETED", "RELEASED", "TOUCHED", "USING", "WATCHING":
			return 0, nil, nil
		case "BURIED":
			return 0, nil, ErrBuried
		case "DEADLINE_SOON":
			return 0, nil, ErrDeadlineSoon
		case "NOT_FOUND":
			return 0, nil, ErrNotFound
		case "NOT_IGNORED":
			return 0, nil, ErrNotIgnored
		case "TIMED_OUT":
			return 0, nil, ErrTimedOut
		}

		return 0, nil, ErrUnexpected
	}()

	// An io.EOF means the connection got disconnected.
	if err == io.EOF {
		return 0, nil, ErrDisconnected
	}

	return id, body, err
}

func (conn *Conn) lcommand(ctx context.Context, format string, params ...interface{}) (uint64, []byte, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	return conn.command(ctx, format, params...)
}

func (conn *Conn) bury(ctx context.Context, job *Job, priority uint32) error {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.bury")
	defer span.End()

	_, _, err := conn.lcommand(ctx, "bury %d %d", job.ID, priority)
	if err == ErrBuried {
		return nil
	}

	return err
}

func (conn *Conn) delete(ctx context.Context, job *Job) error {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.delete")
	defer span.End()

	_, _, err := conn.lcommand(ctx, "delete %d", job.ID)
	return err
}

// Ignore the specified tube.
func (conn *Conn) Ignore(ctx context.Context, tube string) error {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.Ignore")
	defer span.End()

	_, _, err := conn.lcommand(ctx, "ignore %s", tube)
	return err
}

// Put a job in the specified tube.
func (conn *Conn) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.Put")
	defer span.End()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	// If the tube is different than the last time, switch tubes.
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
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.release")
	defer span.End()

	_, _, err := conn.lcommand(ctx, "release %d %d %d", job.ID, priority, delay/time.Second)
	return err
}

// ReserveWithTimeout tries to reserve a job and block for up to a maximum of
// timeout. If no job could be reserved, this function will return without a
// job or error.
func (conn *Conn) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (*Job, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.ReserveWithTimeout")
	defer span.End()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	reservedAt := time.Now()
	id, body, err := conn.command(ctx, "reserve-with-timeout %d", timeout/time.Second)
	switch {
	case err == ErrDeadlineSoon:
		return nil, nil
	case err == ErrNotFound:
		return nil, nil
	case err == ErrTimedOut:
		return nil, nil
	case err != nil:
		return nil, err
	}

	job := &Job{ID: id, Body: body, ReservedAt: reservedAt, conn: conn}

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
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.touch")
	defer span.End()

	touchedAt := time.Now()
	if _, _, err := conn.lcommand(ctx, "touch %d", job.ID); err != nil {
		return err
	}

	// TimeLeft is always 1 second less than the TTR.
	job.Stats.TimeLeft = job.Stats.TTR - time.Second
	job.ReservedAt = touchedAt

	return nil
}

// Watch the specified tube.
func (conn *Conn) Watch(ctx context.Context, tube string) error {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Conn.Watch")
	defer span.End()

	// This check is performed here instead of server-side, because if the name
	// is too long the server return both a BAD_FORMAT and an UNKNOWN_COMMAND
	// response that makes parsing more difficult.
	if len(tube) > 200 {
		return ErrTubeTooLong
	}

	_, _, err := conn.lcommand(ctx, "watch %s", tube)
	return err
}
