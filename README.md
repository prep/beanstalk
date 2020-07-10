# Package beanstalk
`import "github.com/prep/beanstalk"`

## Overview
Package beanstalk provides a beanstalk client.

### Producer

The Producer is used to put jobs into tubes. It provides a connection pool:

```go
producer, err := beanstalk.NewProducer([]string{"localhost:11300"}, beanstalk.Config{
	// Multiply the list of URIs to create a larger pool of connections.
	Multiply: 3,
})
if err != nil {
	// handle error
}
defer producer.Stop()
```

Putting a job in a tube is done by calling Put, which will select a random connection for its operation:

```go
// Create the put parameters. These can be reused between Put calls.
params := beanstalk.PutParams{Priority: 1024, Delay: 0, TTR: 30 * time.Second}

// Put the "Hello World" message in the "mytube" tube.
id, err := producer.Put(ctx, "mytube", []byte("Hello World"), params)
if err != nil {
	// handle error
}
```

If a Put operation fails on a connection, another connection in the pool will be selected for a retry.

### Consumer

The Consumer is used to reserve jobs from tubes. It provides a connection pool:

```go
consumer, err := beanstalk.NewConsumer([]string{"localhost:11300"}, []string{"mytube"}, beanstalk.Config{
	// Multiply the list of URIs to create a larger pool of connections.
	Multiply: 3,
	// NumGoroutines is the number of goroutines that the Receive method will
	// spin up to process jobs concurrently.
	NumGoroutines: 30,
})
if err != nil {
	// handle error
}
```

The ratio of Multiply and NumGoroutines is important. Multiply determines the size of the connection pool and NumGoroutines determines how many reserved jobs you have in-flight. If you have a limited number of connections, but a high number of reserved jobs in-flight, your TCP connection pool might experience congestion and your processing speed will suffer. Although the ratio depends on the speed by which jobs are processed, a good rule of thumb is 1:10.

Reserve jobs from the tubes specified in NewConsumer is done by calling Receive, which will reserve jobs on any of the connections in the pool:

```go
// Call the inline function for every job that was reserved.
consumer.Receive(ctx, func(ctx context.Context, job *beanstalk.Job) {
	// handle job

	if err := job.Delete(ctx); err != nil {
		// handle error
	}
})
```

If the context passed to Receive is cancelled, Receive will finish processing the jobs it has reserved before returning.

### Job

When Receive offers a job the goroutine is responsible for processing that job and finishing it up. A job can either be deleted, released or buried:

```go
// Delete a job, when processing was successful.
err = job.Delete(ctx)

// Release a job, putting it back in the queue for another worker to pick up.
err = job.Release(ctx)

// Release a job, but put it back with a custom priority and a delay before
// it's offered to another worker.
err = job.ReleaseWithParams(ctx, 512, 5 * time.Second)

// Bury a job, when it doesn't need to be processed but needs to be kept
// around for manual inspection or manual requeuing.
err = job.Bury(ctx)
```

### Conn

If the Producer and Consumer abstractions are too high, then Conn provides the lower level abstraction of a single connection to a beanstalk server:

```go
conn, err := beanstalk.Dial("localhost:11300", beanstalk.Config{}))
if err != nil {
	// handle error
}
defer conn.Close()

// conn.Put(...)
// conn.Watch(...)
// conn.Reserve(...)
```

### Logging

The Config structure offers hooks for info and error logs that allows hooking in to a custom log solution.

```go
config := beanstalk.Config{
	InfoFunc: func(message string) {
		log.Info(message)
	},
	ErrorFunc: func(err error, message string) {
		log.WithError(err).Error(message)
	},
}
```

### URIs

NewProducer, NewConsumer and Dial take a URI or a list of URIs as their first argument, who can be described in various formats. In the above examples the beanstalk server was referenced by the host:port notation. This package also supports URI formats like beanstalk:// for a plaintext connection, and beanstalks:// or tls:// for encrypted connections.

In the case of encrypted connections, if no port has been specified it will default to port 11400 as opposed to the default 11300 port.

