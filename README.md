Beanstalk client for Go
=======================
This repository contains a beanstalk package for Go that works with producers, to insert jobs into a beanstalk tube, and consumers, to reserve and delete jobs from a beanstalk tube. Pools with multiple producers and consumers can be created to balance the requests over multiple connections.

Each producer and consumer maintains its own connection to the beanstalk server and will disconnect and reconnect when it detects an unrecoverable error. Timeouts can be set on read and write operations to make sure that an interrupted connecting gets detected early and your application doesn't block as long as the socket timeout of the kernel.

A consumer will also try to keep the reserveration of a job, until you decided to either bury, delete or release that job.

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
pool := beanstalk.NewProducerPool([]string{"127.0.0.1:11300"}, nil)
defer pool.Stop()

// Reusable put parameters.
putParams := &beanstalk.PutParams{1024, 0, 5}

// Insert a job containing "Hello World" in the beanstalk tube named "test".
id, err := pool.Put("test", []byte("Hello World"), putParams)
if err != nil {
    fmt.Println(err)
    return
}

fmt.Printf("Created job with id: %d\n", id)
```

#### ConsumerPool
A ConsumerPool creates 1 or more consumers that connects to a beanstalk server with the purpose of reserving jobs. Here is an example of a *ConsumerPool{}* with a connection to the beanstalk server on *127.0.0.1:11300* that watches tube *test* for jobs to reserve.

```go
// Create a consumer pool with 1 consumer, watching 1 tube.
pool := beanstalk.NewConsumerPool([]string{"127.0.0.1:11300"}, []string{"test"}, nil)
defer pool.Stop()

pool.Play()

for {
    select {
    case job := <-pool.C:
        fmt.Printf("Received job with id: %d\n", job.ID)

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
When you receive a job on your consumer channel, you don't need to worry about the TTR of that job. Each *Consumer{}* object will maintain a reservation of that job by touching it on the beanstalk server until you've finalized it.

To finalize a job, the following functions are available on the *Job{}* object:
```go
type DummyInterfaceForJob interface {
    Bury() error
    BuryWithPriority(priority uint32) error
    Delete() error
    Release() error
    ReleaseWithParams(priority uint32, delay time.Duration) error
}
```

The *Bury()* and *Release()* functions use the priority with which the job was inserted in the first place and *Release()* uses a delay of 0, meaning immediately.

Options
-------
An **Options** struct can be provided at the end of each *NewProducer()*, *NewProducerPool()*, *NewConsumer()* and *NewConsumerPool()* function. It allows you to finetune some behaviour under the hood.

```go
options := &beanstalk.Options{
    QueueSize:        1,

    ReserveTimeout:   3 * time.Second,
    ReconnectTimeout: 3 * time.Second,
    ReadWriteTimeout: 5 * time.Second,

    InfoLog:          log.New(os.Stdout, "INFO: ", 0),
    ErrorLog:         log.New(os.Stderr, "ERROR: ", 0),
}

producerPool := beanstalk.NewProducerPool([]string{"127.0.0.1:11300"}, options)
consumerPool := beanstalk.NewConsumerPool([]string{"127.0.0.1:11300"}, []string{"test"}, options)
```

* **QueueSize** defines the number of beanstalk jobs a single Consumer can reserve and maintain. This is useful if you want to deal with multiple jobs at the same time.
* **ReserveTimeout** defines how long a beanstalk reserve command should wait before it should timeout. The default and minimum value is 1 second.
* **ReconnectTimeout** defines how long a producer or consumer should wait between reconnect attempts. The default is 3 seconds, with a minimum of 1 second.
* **ReadWriteTimeout** defines how long each Read() or Write() operation is allowed to block until the connection is considered broken. The default is disabled and the minimum value is 1ms.
* **InfoLog** is used to log info messages to, but can be nil.
* **ErrorLog** is used to log error messages to, but can be nil.

License
-------
This software is created for MessageBird B.V. and licensed under [The ISC License](http://opensource.org/licenses/ISC). Copyright (c) 2015, Maurice Nonnekes <maurice@messagebird.com>.
