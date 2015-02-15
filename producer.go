package beanstalk

// Producer sets up a connection to a beanstalk server and takes jobs it
// receives on the put channel to insert into beanstalk.
type Producer struct {
	Client
	putCh chan *Put
	stop  chan struct{}
}

// NewProducer returns a new Producer object.
func NewProducer(socket string, putCh chan *Put, options *Options) *Producer {
	producer := &Producer{
		Client: NewClient(socket, options),
		putCh:  putCh,
		stop:   make(chan struct{}, 1)}

	go producer.connectionManager()
	return producer
}

// Stop shuts down this producer from running.
func (producer *Producer) Stop() {
	producer.stop <- struct{}{}
}

// connectionManager is responsible for maintaining a connection to the
// beanstalk server and inserting jobs via the 'put' command.
func (producer *Producer) connectionManager() {
	var lastTube string
	var putCh chan *Put

	producer.OpenConnection()
	defer producer.CloseConnection()

	for {
		select {
		// This case handles new 'put' requests.
		case job := <-putCh:
			if job.Tube != lastTube {
				if err := producer.Use(job.Tube); err != nil {
					job.Response <- PutResponse{0, err}
					break
				}

				lastTube = job.Tube
			}

			// Insert the job into beanstalk and return the response.
			id, err := producer.Put(job)
			job.Response <- PutResponse{id, err}

		// If a new connection was created, set it up here.
		case conn := <-producer.connCreatedC:
			producer.SetConnection(conn)
			putCh = producer.putCh

		case <-producer.connClosedC:
			lastTube = ""
			putCh = nil

		// Close the connection and stop this goroutine from running.
		case <-producer.stop:
			return
		}
	}
}
