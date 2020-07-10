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
	// NumGoroutines is the number of goroutines that the Receive method will
	// spin up to process jobs concurrently.
	NumGoroutines: 10,
	// ReserveTimeout is the time a consumer connection waits between reserve
	// attempts if the last attempt failed to reserve a job.
	ReserveTimeout: 1 * time.Second,
})
if err != nil {
	// handle error
}
```

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

### Conn

For direct operations on a single connection a Conn can be created:

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

