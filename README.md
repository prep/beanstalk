beanstalk
[![TravisCI](https://travis-ci.org/prep/beanstalk.svg?branch=master)](https://travis-ci.org/prep/beanstalk.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/prep/beanstalk)](https://goreportcard.com/report/github.com/prep/beanstalk)
[![GoDoc](https://godoc.org/github.com/prep/beanstalk?status.svg)](https://godoc.org/github.com/prep/beanstalk)
=========
This repository contains a beanstalk package for Go that works with producers to insert jobs into a beanstalk tube, and consumers to reserve and delete jobs from a beanstalk tube. Pools with multiple producers and consumers can be created to balance the requests over multiple connections.

Each producer and consumer maintains its own connection to the beanstalk server and will disconnect and reconnect when it detects an unrecoverable error. Timeouts can be set on read and write operations to make sure that an interrupted connecting gets detected early and your application doesn't block as long as the connection timeout of the kernel.

Examples
--------
To get started, you need to import the client into your Go project:

```go
import "github.com/prep/beanstalk"
```

The easiest way to work with this client is by creating a *ProducerPool{}* and/or *ConsumerPool{}*.

#### ProducerPool
A ProducerPool creates 1 or more producers that connects to a beanstalk server with the purpose of feeding it *put* requests. Here is an example of a *ProducerPool{}* with a connection to the beanstalk server on *127.0.0.1:11300*:

```go
// Create a producer pool with 1 producer.
pool, err := beanstalk.NewProducerPool([]string{"beanstalk://127.0.0.1:11300"}, nil)
if err != nil {
  log.Fatal("Unable to create beanstalk producer pool: %s", err)
}
defer pool.Stop()

// Reusable put parameters.
putParams := &beanstalk.PutParams{1024, 0, 5}

// Insert a job containing "Hello World" in the beanstalk tube named "test".
id, err := pool.Put("test", []byte("Hello World"), putParams)
if err != nil {
  return err
}

log.Printf("Created job with id: %d", id)
```

#### ConsumerPool
A ConsumerPool creates 1 or more consumers that connects to a beanstalk server with the purpose of reserving jobs. Here is an example of a *ConsumerPool{}* with a connection to the beanstalk server on *127.0.0.1:11300* that watches tube *test* for jobs to reserve.

```go
// Create a consumer pool with 1 consumer, watching 1 tube.
pool, err := beanstalk.NewConsumerPool([]string{"beanstalk://127.0.0.1:11300"}, []string{"test"}, nil)
if err != nil {
  log.Fatal("Unable to create beanstalk consumer pool: %s", err)
}
defer pool.Stop()

pool.Play()

for {
    select {
    case job := <-pool.C:
        log.Printf("Received job with id: %d", job.ID)

        if err := doSomethingWithJob(job); err != nil {
            fmt.Printf("Burying job %d with body: %s\n", job.ID, string(job.Body))
            job.Bury()
        } else {
            job.Delete()
        }
    // ...
    }
}
```

Managing consumers
------------------
By default, *Consumer{}* and *ConsumerPool{}* objects start out in a paused state, which means that even though they will try to establish a connection to the beanstalk server immediately, they will not reserve any jobs until the *Play()* function has been called. If you want to stop the stream of reserved jobs for a moment, you can call the *Pause()* function. It should be noted that calling *Pause()* won't affect jobs that are currently reserved.

Jobs
----
Jobs are offered on your consumer channel and it looks like this:

```go
type Job struct {
	ID        uint64
	Body      []byte
	Priority  uint32
	TTR       time.Duration
}
```

When you receive a *Job{}* on your consumer channel, it is your responsibility to honor the TTR of that job. To do that, you call the *TouchAt()* function to get the remaining TTR of the current job, which has a margin built in for safety. You can use the *Touch()* function to refresh the TTR of that job.

```go
Touch() error
TouchAt() time.Duration
```

To finalize a job, the following functions are available on the *Job{}* object:
```go
Bury() error
BuryWithPriority(priority uint32) error
Delete() error
Release() error
ReleaseWithParams(priority uint32, delay time.Duration) error
```

The *Bury()* and *Release()* functions use the priority with which the job was inserted in the first place and *Release()* uses a delay of 0, meaning immediately.

Options
-------
An **Options** struct can be provided at the end of each *NewProducer()*, *NewProducerPool()*, *NewConsumer()* and *NewConsumerPool()* function. It allows you to fine-tune some behaviour under the hood.

```go
options := &beanstalk.Options{
  // ReserveTimeout defines how long a beanstalk reserve command should wait
  // before it should timeout. The default and minimum value is 1 second.
  ReserveTimeout: 3 * time.Second,
  // ReconnectTimeout defines how long a producer or consumer should wait
  // between reconnect attempts. The default is 3 seconds, with a minimum of 1
  // second.
  ReconnectTimeout: 3 * time.Second,
  // ReadWriteTimeout defines how long each read or write operation is  allowed
  // to block until the connection is considered broken. The default is
  // disabled and the minimum value is 1ms.
  ReadWriteTimeout: 5 * time.Second,

  // InfoLog is used to log info messages to, but can be nil.
  InfoLog: log.New(os.Stdout, "INFO: ", 0),
  // ErrorLog is used to log error messages to, but can be nil.
  ErrorLog: log.New(os.Stderr, "ERROR: ", 0),
}

producerPool, _ := beanstalk.NewProducerPool([]string{"beanstalk://127.0.0.1:11300"}, options)
consumerPool, _ := beanstalk.NewConsumerPool([]string{"beanstalk://127.0.0.1:11300"}, []string{"test"}, options)
```

License
-------
This software is created for MessageBird B.V. and distributed under the BSD-style license found in the LICENSE file.
