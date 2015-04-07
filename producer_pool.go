package beanstalk

// ProducerPool maintains a pool of Producers with the purpose of spreading
// incoming Put requests over the maintained Producers.
type ProducerPool struct {
	producers []*Producer
	putC      chan *Put
	putTokens chan *Put
	options   Options
}

// NewProducerPool creates a pool of Producer objects.
func NewProducerPool(sockets []string, options Options) *ProducerPool {
	pool := &ProducerPool{putC: make(chan *Put), options: SanitizeOptions(options)}
	pool.putTokens = make(chan *Put, len(sockets))

	for _, socket := range sockets {
		pool.producers = append(pool.producers, NewProducer(socket, pool.putC, options))
		pool.putTokens <- NewPut(pool.putC)
	}

	return pool
}

// Stop shuts down all the producers in the pool.
func (pool *ProducerPool) Stop() {
	for i, producer := range pool.producers {
		producer.Stop()
		pool.producers[i] = nil
	}
}

// Put inserts a new job into beanstalk.
func (pool *ProducerPool) Put(tube string, body []byte, params *PutParams) (uint64, error) {
	put := <-pool.putTokens
	put.SendRequest(tube, body, params)

	id, err := put.ReceiveResponse()
	pool.putTokens <- put

	return id, err
}
