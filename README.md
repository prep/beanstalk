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

The easiest way to work with this client is by creating a ProducerPool and/or ConsumerPool. A pool has 1 or more connections to a beanstalk server.

#### ProducerPool
A ProducerPool creates 1 or more producers that connects to a beanstalk server with the purpose of feeding it **put** requests. Here is an example of a ProducerPool with a connection to the beanstalk server on **127.0.0.1:11300**:

```go
// Create a producer pool with 1 producer.
pool := beanstalk.NewProducerPool([]string{"127.0.0.1:11300"}, nil)

// Reusable put parameters.
putParams := &beanstalk.PutParams{1024, 0, 5}

// Put a job containing 'Hello World' in the 'test' tube.
id, err := pool.Put("test", []byte("Hello World"), putParams)
if err != nil {
    fmt.Println(err)
    return
}

fmt.Printf("Created job with id: %d\n", id)

// Disconnect and stop the producers.
pool.Stop()
```

#### ConsumerPool
A ConsumerPool creates 1 or more consumers that connects to a beanstalk server with the purpose of reserving jobs. Here is an example of a ConsumerPool with a connection to the beanstalk server on **127.0.0.1:11300** that watches tube **test** for jobs to reserve.

```go
// Create a consumer pool with 1 consumer, watching 1 tube.
pool := beanstalk.NewConsumerPool([]string{"127.0.0.1:11300"}, []string{"test"}, nil)

for {
    select {
    case job := <-pool.C:
        fmt.Printf("Received job with id: %d\n", job.ID)

        if err := doSomethingWithJob(job); err != nil {
            job.Bury()
            // job.Release()
        } else {
            job.Delete()
        }
    }
}
```

#### Options
An **Options** struct can be provided at the end of each **NewProducerPool()** and **NewConsumerPool()** call. It allows you to finetune some behaviour under the hood.

```go
options := &beanstalk.Options{
    ReserveTimeout:   3 * time.Second,
    ReconnectTimeout: 3 * time.Second,
    ReadWriteTimeout: 5 * time.Second,
}

producerPool := beanstalk.NewProducerPool([]string{"127.0.0.1:11300"}, options)
consumerPool := beanstalk.NewConsumerPool([]string{"127.0.0.1:11300"}, []string{"test"}, options)
```

* **ReserveTimeout** defines how long a beanstalk reserve command should wait before it should timeout. The default and minimum value is 1 second.
* **ReconnectTimeout** defines how long a producer or consumer should wait between reconnect attempts. The default is 3 seconds, with a minimum of 1 second.
* **ReadWriteTimeout** defines how long each Read() or Write() operation is allowed to block until the connection is considered broken. The default is disabled and the minimum value is 1ms.

License
-------
This software is licensed under [The BSD 2-Clause License](http://opensource.org/licenses/BSD-2-Clause). Copyright (c) 2015, MessageBird
