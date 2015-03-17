package beanstalk

// Producer puts the jobs it receives on its channel into beanstalk.
type Producer struct {
	client *Client
	putCh  chan *Put
	stop   chan struct{}
}

// NewProducer returns a new Producer object.
func NewProducer(socket string, putCh chan *Put, options *Options) *Producer {
	producer := &Producer{
		putCh: putCh,
		stop:  make(chan struct{}, 1),
	}

	go producer.jobManager(socket, options)
	return producer
}

// Stop this producer running.
func (producer *Producer) Stop() {
	producer.stop <- struct{}{}
}

// jobManager is responsible for accepting new put requests and inserting them
// into beanstalk.
func (producer *Producer) jobManager(socket string, options *Options) {
	var lastTube string
	var putCh chan *Put
	var isConnected = false

	client := NewClient(socket, options)
	defer client.Close()

	for {
		select {
		// This case handles new 'put' requests.
		case job := <-putCh:
			if job.Tube != lastTube {
				if err := client.Use(job.Tube); err != nil {
					job.Response <- PutResponse{0, err}
					break
				}

				lastTube = job.Tube
			}

			// Insert the job into beanstalk and return the response.
			id, err := client.Put(job)
			job.Response <- PutResponse{id, err}

		case isConnected = <-client.Connected:
			lastTube = ""
			if isConnected {
				putCh = producer.putCh
			} else {
				putCh = nil
			}

		// Close the connection and stop this goroutine from running.
		case <-producer.stop:
			return
		}
	}
}
