package beanstalk

import "sync"

// Producer puts the jobs it receives on its channel into beanstalk.
type Producer struct {
	url       string
	putC      chan *Put
	stop      chan struct{}
	isStopped bool
	options   *Options
	startOnce sync.Once
	stopOnce  sync.Once
	mu        sync.Mutex
}

// NewProducer returns a new Producer object.
func NewProducer(url string, putC chan *Put, options *Options) (*Producer, error) {
	if options == nil {
		options = DefaultOptions()
	}

	if _, _, err := ParseURL(url); err != nil {
		return nil, err
	}

	return &Producer{
		url:     url,
		putC:    putC,
		stop:    make(chan struct{}, 1),
		options: options,
	}, nil
}

// Start this producer.
func (producer *Producer) Start() {
	producer.startOnce.Do(func() {
		go producer.manager()
	})
}

// Stop this producer.
func (producer *Producer) Stop() {
	producer.stopOnce.Do(func() {
		producer.mu.Lock()
		producer.isStopped = true
		close(producer.stop)
		producer.mu.Unlock()
	})
}

// manager is responsible for accepting new put requests and inserting them
// into beanstalk.
func (producer *Producer) manager() {
	var client *Client
	var lastTube string
	var putC chan *Put

	// Set up a new connection.
	newConnection, abortConnect := connect(producer.url, producer.options)

	// Close the client and reconnect.
	reconnect := func(format string, a ...interface{}) {
		producer.options.LogError(format, a...)

		if client != nil {
			client.Close()
			client, putC, lastTube = nil, nil, ""
			producer.options.LogInfo("Producer connection closed. Reconnecting")
			newConnection, abortConnect = connect(producer.url, producer.options)
		}
	}

	for {
		select {
		// Set up a new beanstalk client connection.
		case conn := <-newConnection:
			client, abortConnect = NewClient(conn, producer.options), nil
			putC = producer.putC

		// This case handles new 'put' requests.
		case put := <-putC:
			request := &put.request

			if request.Tube != lastTube {
				if err := client.Use(request.Tube); err != nil {
					put.Response(0, err)
					reconnect("Unable to use tube '%s': %s", request.Tube, err)
					break
				}

				lastTube = request.Tube
			}

			// Insert the job into beanstalk and return the response.
			id, err := client.Put(request)
			if err != nil {
				reconnect("Unable to put job into beanstalk: %s", err)
			}

			put.Response(id, err)

		// Close the connection and stop this goroutine from running.
		case <-producer.stop:
			if client != nil {
				client.Close()
			}

			if abortConnect != nil {
				close(abortConnect)
			}

			return
		}
	}
}
