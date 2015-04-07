package beanstalk

// Producer puts the jobs it receives on its channel into beanstalk.
type Producer struct {
	client *Client
	putC   chan *Put
	stop   chan struct{}
}

// NewProducer returns a new Producer object.
func NewProducer(socket string, putC chan *Put, options Options) *Producer {
	producer := &Producer{
		putC: putC,
		stop: make(chan struct{}, 1),
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
	var putC chan *Put

	// Set up a new connection.
	newConnection, abortConnect := Connect(socket, options)

	// Close the client and reconnect.
	reconnect := func() {
		if client != nil {
			client.Close()
			client, putC, lastTube = nil, nil, ""
			options.LogInfo("Producer connection closed. Reconnecting")
			newConnection, abortConnect = Connect(socket, options)
		}
	}

	for {
		select {
		// This case handles new 'put' requests.
		case put := <-putC:
			request := &put.request

			if request.Tube != lastTube {
				if err := client.Use(request.Tube); err != nil {
					put.Response(0, err)
					options.LogError("Unable to use tube '%s': %s", request.Tube, err)
					reconnect()
					break
				}

				lastTube = request.Tube
			}

			// Insert the job into beanstalk and return the response.
			id, err := client.Put(request)
			if err != nil {
				options.LogError("Unable to put job into beanstalk: %s", err)
				reconnect()
			}

			put.Response(id, err)

		case conn := <-newConnection:
			client, abortConnect = NewClient(conn, options), nil
			putC = producer.putC

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
