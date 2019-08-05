/*
Package beanstalk implements a beanstalk client that includes various
abstractions to make producing and consuming jobs easier.

Create a Conn if you want the most basic version of a beanstalk client:

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

In most cases it is easier to leverage ConsumerPool and ProducerPool to manage
one or more beanstalk client connections, as this provides some form of
load balacning and auto-reconnect mechanisms under the hood.

The ProducerPool manages one or more client connections used specifically for
producing beanstalk jobs. If exports a Put method that load balances between the
available connections

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

A ConsumerPool manages one or more client connections used specifically for
consuming beanstalk jobs. If exports a channel on which Job types can be read.

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

Alternatively, instead of leveraging the exported channel it is possible to
provide a handler function that is called for every reserved beanstalk job by
calling the Receive method on ConsumerPool.

	pool.Play()
	pool.Receive(ctx, func(ctx context.Context, job *beanstalk.Job) {
		// process job

		if err = job.Delete(ctx); err != nil {
			// handle error
		}
	})

In the above examples the beanstalk server was referenced by way of the
host:port notation. This package also supports URI formats like beanstalk:// for
a plaintext connection, and beanstalks:// or tls:// for encrypted connections.
*/
package beanstalk
