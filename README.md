[![TravisCI](https://travis-ci.org/prep/beanstalk.svg?branch=master)](https://travis-ci.org/prep/beanstalk.svg?branch=master)
[![GoDoc](https://godoc.org/github.com/prep/beanstalk?status.svg)](https://godoc.org/github.com/prep/beanstalk)

# Package beanstalk
`import "github.com/prep/beanstalk"`

[Overview](#user-content-overview)  
[Index](#user-content-index)  

## Overview
Package beanstalk implements a beanstalk client that includes various
abstractions to make producing and consuming jobs easier.

Create a Conn if you want the most basic version of a beanstalk client:

```go
conn, err := beanstalk.Dial("localhost:11300", beanstalk.Config{})
if err != nil {
	// handle error
}
defer conn.Close()

id, err := conn.Put(ctx, "example_tube", []byte("Hello World"), beanstalk.PutParams{
	Priority: 1024,
	Delay:    2 * time.Second,
	TTR:      1 * time.Minute,
})
if err != nil {
	// handle error
}

if err = conn.Watch(ctx, "example_tube"); err != nil {
	// handle error
}

job, err := conn.ReserveWithTimeout(ctx, 3*time.Second)
if err != nil {
	// handle error
}

// process job

if err = job.Delete(ctx); err != nil {
	// handle error
}
```

In most cases it is easier to leverage ConsumerPool and ProducerPool to manage
one or more beanstalk client connections, as this provides some form of
load balacning and auto-reconnect mechanisms under the hood.

The ProducerPool manages one or more client connections used specifically for
producing beanstalk jobs. If exports a Put method that load balances between the
available connections

```go
pool, err := beanstalk.NewProducerPool([]string{"localhost:11300"}, beanstalk.Config{})
if err != nil {
	// handle error
}
defer pool.Stop()

id, err := pool.Put(ctx, "example_tube", []byte("Hello World"), beanstalk.PutParams{
	Priority: 1024,
	Delay:    2 * time.Second,
	TTR:      1 * time.Minute,
}
```

A ConsumerPool manages one or more client connections used specifically for
consuming beanstalk jobs. If exports a channel on which Job types can be read.

```go
pool, err := beanstalk.NewConsumerPool([]string{"localhost:11300"}, []string{"example_tube"}, beanstalk.Config{})
if err != nil {
	// handle error
}
defer pool.Stop()

pool.Play()
for job := range pool.C {
	// process job

	if err = job.Delete(ctx); err != nil {
		// handle error
	}
}
```

Alternatively, instead of leveraging the exported channel it is possible to
provide a handler function that is called for every reserved beanstalk job by
calling the Receive method on ConsumerPool.

```go
pool.Play()
pool.Receive(ctx, func(ctx context.Context, job *beanstalk.Job) {
	// process job

	if err = job.Delete(ctx); err != nil {
		// handle error
	}
})
```

In the above examples the beanstalk server was referenced by way of the
host:port notation. This package also supports URI formats like beanstalk:// for
a plaintext connection, and beanstalks:// or tls:// for encrypted connections.

## Index
[Variables](#user-content-variables)  
[func ParseURI(uri string) (string, bool, error)](#user-content-func-ParseURI)
[type Config](#user-content-type-Config)  

[type Conn](#user-content-type-Conn)  
  [func Dial(uri string, config Config) (*Conn, error)](#user-content-func-Conn-Dial)  
  [func (conn *Conn) Close() error](#user-content-method-Conn-Close)  
  [func (conn *Conn) Ignore(ctx context.Context, tube string) error](#user-content-method-Conn-Ignore)  
  [func (conn *Conn) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)](#user-content-method-Conn-Put)  
  [func (conn *Conn) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (*Job, error)](#user-content-method-Conn-ReserveWithTimeout)  
  [func (conn *Conn) String() string](#user-content-method-Conn-String)  
  [func (conn *Conn) Watch(ctx context.Context, tube string) error](#user-content-method-Conn-Watch)  

[type Consumer](#user-content-type-Consumer)  
  [func NewConsumer(uri string, tubes []string, config Config) (*Consumer, error)](#user-content-func-Consumer-NewConsumer)  
  [func (consumer *Consumer) Close()](#user-content-method-Consumer-Close)  
  [func (consumer *Consumer) Pause()](#user-content-method-Consumer-Pause)  
  [func (consumer *Consumer) Play()](#user-content-method-Consumer-Play)  
  [func (consumer *Consumer) Receive(ctx context.Context, fn func(ctx context.Context, job *Job))](#user-content-method-Consumer-Receive)  

[type ConsumerPool](#user-content-type-ConsumerPool)  
  [func NewConsumerPool(uris []string, tubes []string, config Config) (*ConsumerPool, error)](#user-content-func-ConsumerPool-NewConsumerPool)  
  [func (pool *ConsumerPool) Pause()](#user-content-method-ConsumerPool-Pause)  
  [func (pool *ConsumerPool) Play()](#user-content-method-ConsumerPool-Play)  
  [func (pool *ConsumerPool) Receive(ctx context.Context, fn func(ctx context.Context, job *Job))](#user-content-method-ConsumerPool-Receive)  
  [func (pool *ConsumerPool) Stop()](#user-content-method-ConsumerPool-Stop)  

[type Job](#user-content-type-Job)  
  [func (job *Job) Bury(ctx context.Context) error](#user-content-method-Job-Bury)  
  [func (job *Job) BuryWithPriority(ctx context.Context, priority uint32) error](#user-content-method-Job-BuryWithPriority)  
  [func (job *Job) Delete(ctx context.Context) error](#user-content-method-Job-Delete)  
  [func (job *Job) Release(ctx context.Context) error](#user-content-method-Job-Release)  
  [func (job *Job) ReleaseWithParams(ctx context.Context, priority uint32, delay time.Duration) error](#user-content-method-Job-ReleaseWithParams)  
  [func (job *Job) Touch(ctx context.Context) error](#user-content-method-Job-Touch)  
  [func (job *Job) TouchAfter() time.Duration](#user-content-method-Job-TouchAfter)  

[type Producer](#user-content-type-Producer)  
  [func NewProducer(uri string, config Config) (*Producer, error)](#user-content-func-Producer-NewProducer)  
  [func (producer *Producer) Close()](#user-content-method-Producer-Close)  
  [func (producer *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)](#user-content-method-Producer-Put)  

[type ProducerPool](#user-content-type-ProducerPool)  
  [func NewProducerPool(uris []string, config Config) (*ProducerPool, error)](#user-content-func-ProducerPool-NewProducerPool)  
  [func (pool *ProducerPool) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)](#user-content-method-ProducerPool-Put)  
  [func (pool *ProducerPool) Stop()](#user-content-method-ProducerPool-Stop)  

[type PutParams](#user-content-type-PutParams)  

## Package files
[beanstalk.go](beanstalk.go)
[config.go](config.go)
[conn.go](conn.go)
[consumer.go](consumer.go)
[consumer_pool.go](consumer_pool.go)
[doc.go](doc.go)
[job.go](job.go)
[producer.go](producer.go)
[producer_pool.go](producer_pool.go)

## Variables
These error may be returned by any of Conn's methods.

```go
var (
	ErrBuried	= errors.New("job was buried")
	ErrDeadlineSoon	= errors.New("deadline soon")
	ErrDisconnected	= errors.New("client disconnected")
	ErrNotFound	= errors.New("job not found")
	ErrTimedOut	= errors.New("reserve timed out")
	ErrNotIgnored	= errors.New("tube not ignored")
	ErrTubeTooLong	= errors.New("tube name too long")
	ErrUnexpected	= errors.New("unexpected response received")
)
```

ErrJobFinished is returned when a job was already finished.

```go
var ErrJobFinished = errors.New("job was already finished")
```

### func ParseURI

```go
func ParseURI(uri string) (string, bool, error)
```
ParseURI returns the socket of the specified URI and if the connection is
supposed to be a TLS or plaintext connection. Valid URI schemes are:

```go
beanstalk://host:port
beanstalks://host:port
tls://host:port
```

Where both the beanstalks and tls scheme mean the same thing. Alternatively,
it is also possibly to just specify the host:port combo which is assumed to
be a plaintext connection.

### type Config
A Config structure is used to configure a Consumer, Producer, one of its
pools or Conn.

```go
type Config struct {
	// NumGoroutines is the number of goroutines that the Receive() method will
	// spin up.
	// The default is to spin up 1 goroutine.
	NumGoroutines	int
	// ReserveTimeout is the time a consumer should wait before reserving a job,
	// when the last attempt didn't yield a job.
	// The default is to wait 5 seconds.
	ReserveTimeout	time.Duration
	// ReleaseTimeout is the time a consumer should hold a reserved job before
	// it is released back.
	// The default is to wait 3 seconds.
	ReleaseTimeout	time.Duration
	// ReconnectTimeout is the timeout between reconnects.
	// The default is to wait 10 seconds.
	ReconnectTimeout	time.Duration
	// TLSConfig describes the configuration that is used when Dial() makes a
	// TLS connection.
	TLSConfig	*tls.Config
	// InfoLog is used to log informational messages.
	InfoLog	*log.Logger
	// ErrorLog is used to log error messages.
	ErrorLog	*log.Logger
	// contains filtered or unexported fields
}
```


### type Conn
Conn describes a connection to a beanstalk server.

```go
type Conn struct {
	URI string
	// contains filtered or unexported fields
}
```

#### func Dial

```go
func Dial(uri string, config Config) (*Conn, error)
```
Dial into a beanstalk server.

#### func (*Conn) Close

```go
func (conn *Conn) Close() error
```
Close this connection.

#### func (*Conn) Ignore

```go
func (conn *Conn) Ignore(ctx context.Context, tube string) error
```
Ignore the specified tube.

#### func (*Conn) Put

```go
func (conn *Conn) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)
```
Put a job in the specified tube.

#### func (*Conn) ReserveWithTimeout

```go
func (conn *Conn) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (*Job, error)
```
ReserveWithTimeout tries to reserve a job and block for up to a maximum of
timeout. If no job could be reserved, this function will return without a
job or error.

#### func (*Conn) String

```go
func (conn *Conn) String() string
```

#### func (*Conn) Watch

```go
func (conn *Conn) Watch(ctx context.Context, tube string) error
```
Watch the specified tube.


### type Consumer
Consumer maintains a connnection to a beanstalk server and offers up jobs
on its exposed jobs channel. When it gets disconnected, it automatically
tries to reconnect.

```go
type Consumer struct {
	// C offers up reserved jobs.
	C <-chan *Job
	// contains filtered or unexported fields
}
```

#### func NewConsumer

```go
func NewConsumer(uri string, tubes []string, config Config) (*Consumer, error)
```
NewConsumer connects to the beanstalk server that's referenced in URI and
returns a Consumer.

#### func (*Consumer) Close

```go
func (consumer *Consumer) Close()
```
Close this consumer's connection.

#### func (*Consumer) Pause

```go
func (consumer *Consumer) Pause()
```
Pause this consumer.

#### func (*Consumer) Play

```go
func (consumer *Consumer) Play()
```
Play unpauses this customer.

#### func (*Consumer) Receive

```go
func (consumer *Consumer) Receive(ctx context.Context, fn func(ctx context.Context, job *Job))
```
Receive calls fn for each job it can reserve on this consumer.


### type ConsumerPool
ConsumerPool manages a pool of consumers that share a single channel on
which jobs are offered.

```go
type ConsumerPool struct {
	// C offers up reserved jobs.
	C <-chan *Job
	// contains filtered or unexported fields
}
```

#### func NewConsumerPool

```go
func NewConsumerPool(uris []string, tubes []string, config Config) (*ConsumerPool, error)
```
NewConsumerPool creates a pool of Consumers from the list of URIs that has
been provided.

#### func (*ConsumerPool) Pause

```go
func (pool *ConsumerPool) Pause()
```
Pause all the consumers in this pool.

#### func (*ConsumerPool) Play

```go
func (pool *ConsumerPool) Play()
```
Play unpauses all the consumers in this pool.

#### func (*ConsumerPool) Receive

```go
func (pool *ConsumerPool) Receive(ctx context.Context, fn func(ctx context.Context, job *Job))
```
Receive calls fn in for each job it can reserve on the consumers in this pool.

#### func (*ConsumerPool) Stop

```go
func (pool *ConsumerPool) Stop()
```
Stop all the consumers in this pool.


### type Job
Job describes a beanstalk job and its stats.

```go
type Job struct {
	ID		uint64
	Body		[]byte
	ReservedAt	time.Time
	Stats		struct {
		PutParams	`yaml:",inline"`
		Tube		string		`yaml:"tube"`
		State		string		`yaml:"state"`
		Age		time.Duration	`yaml:"age"`
		TimeLeft	time.Duration	`yaml:"time-left"`
		File		int		`yaml:"file"`
		Reserves	int		`yaml:"reserves"`
		Timeouts	int		`yaml:"timeouts"`
		Releases	int		`yaml:"releases"`
		Buries		int		`yaml:"buries"`
		Kicks		int		`yaml:"kicks"`
	}
	// contains filtered or unexported fields
}
```

#### func (*Job) Bury

```go
func (job *Job) Bury(ctx context.Context) error
```
Bury this job.

#### func (*Job) BuryWithPriority

```go
func (job *Job) BuryWithPriority(ctx context.Context, priority uint32) error
```
BuryWithPriority buries this job with the specified priority.

#### func (*Job) Delete

```go
func (job *Job) Delete(ctx context.Context) error
```
Delete this job.

#### func (*Job) Release

```go
func (job *Job) Release(ctx context.Context) error
```
Release this job back with its original priority and without delay.

#### func (*Job) ReleaseWithParams

```go
func (job *Job) ReleaseWithParams(ctx context.Context, priority uint32, delay time.Duration) error
```
ReleaseWithParams releases this job back with the specified priority and delay.

#### func (*Job) Touch

```go
func (job *Job) Touch(ctx context.Context) error
```
Touch the job thereby resetting its reserved status.

#### func (*Job) TouchAfter

```go
func (job *Job) TouchAfter() time.Duration
```
TouchAfter returns the duration until this jobs needs to be touched for its
reservation to be retained.


### type Producer
Producer manages a connection for the purpose of inserting jobs.

```go
type Producer struct {
	// contains filtered or unexported fields
}
```

#### func NewProducer

```go
func NewProducer(uri string, config Config) (*Producer, error)
```
NewProducer creates a connection to a beanstalk server, but will return an
error if the connection fails. Once established, the connection will be
maintained in the background.

#### func (*Producer) Close

```go
func (producer *Producer) Close()
```
Close this consumer's connection.

#### func (*Producer) Put

```go
func (producer *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)
```
Put inserts a job into beanstalk.


### type ProducerPool
ProducerPool manages a connection pool of Producers and provides a simple
interface for balancing Put requests over the pool of connections.

```go
type ProducerPool struct {
	// contains filtered or unexported fields
}
```

#### func NewProducerPool

```go
func NewProducerPool(uris []string, config Config) (*ProducerPool, error)
```
NewProducerPool creates a pool of Producers from the list of URIs that has
been provided.

#### func (*ProducerPool) Put

```go
func (pool *ProducerPool) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error)
```
Put a job into the specified tube.

#### func (*ProducerPool) Stop

```go
func (pool *ProducerPool) Stop()
```
Stop all the producers in this pool.


### type PutParams
PutParams are the parameters used to perform a Put operation.

```go
type PutParams struct {
	Priority	uint32		`yaml:"pri"`
	Delay		time.Duration	`yaml:"delay"`
	TTR		time.Duration	`yaml:"ttr"`
}
```
