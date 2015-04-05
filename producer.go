package beanstalk

// Producer puts the jobs it receives on its channel into beanstalk.
type Producer struct {
	client *Client
	putCh  chan *Put
	stop   chan struct{}
}

// NewProducer returns a new Producer object.
func NewProducer(socket string, putCh chan *Put, options Options) *Producer {
	producer := &Producer{
		putCh: putCh,
		stop:  make(chan struct{}, 1),
	}

	go producer.manager(socket, SanitizeOptions(options))
	return producer
}

// Stop this producer.
func (producer *Producer) Stop() {
	producer.stop <- struct{}{}
}

// manager is responsible for accepting new put requests and inserting them
// into beanstalk.
func (producer *Producer) manager(socket string, options Options) {
	var client *Client
	var lastTube string
	var putCh chan *Put

	// Set up a new connection.
	newConnection, abortConnect := Connect(socket, options)

	// Close the client and reconnect.
	reconnect := func() {
		if client != nil {
			client.Close()
			client, putCh, lastTube = nil, nil, ""
			newConnection, abortConnect = Connect(socket, options)
		}
	}

	for {
		select {
		// This case handles new 'put' requests.
		case job := <-putCh:
			if job.Tube != lastTube {
				if err := client.Use(job.Tube); err != nil {
					job.Response <- PutResponse{0, err}
					options.LogError("Error using tube '%s': %s", job.Tube, err)
					reconnect()
					break
				}

				lastTube = job.Tube
			}

			// Insert the job into beanstalk and return the response.
			id, err := client.Put(job)
			if err != nil {
				options.LogError("Error putting job: %s", err)
				reconnect()
			}

			job.Response <- PutResponse{id, err}

		case conn := <-newConnection:
			client, abortConnect = NewClient(conn, options), nil
			putCh = producer.putCh

		// Close the connection and stop this goroutine from running.
		case <-producer.stop:
			if client != nil {
				client.Close()
			}

			if abortConnect != nil {
				abortConnect <- struct{}{}
			}

			return
		}
	}
}
