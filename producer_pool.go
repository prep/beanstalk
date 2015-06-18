package beanstalk

import "sync"

// ProducerPool maintains a pool of Producers with the purpose of spreading
// incoming Put requests over the maintained Producers.
type ProducerPool struct {
	producers []*Producer
	putC      chan *Put
	putTokens chan *Put
	sync.Mutex
}

// NewProducerPool creates a pool of Producer objects.
func NewProducerPool(sockets []string, options *Options) *ProducerPool {
	pool := &ProducerPool{putC: make(chan *Put)}
	pool.putTokens = make(chan *Put, len(sockets))

	for _, socket := range sockets {
		pool.producers = append(pool.producers, NewProducer(socket, pool.putC, options))
		pool.putTokens <- NewPut(pool.putC, options)
	}

	return pool
}

// Stop shuts down all the producers in the pool.
func (pool *ProducerPool) Stop() {
	pool.Lock()
	defer pool.Unlock()

	for i, producer := range pool.producers {
		producer.Stop()
		pool.producers[i] = nil
	}
	pool.producers = []*Producer{}
}

// Put inserts a new job into beanstalk.
func (pool *ProducerPool) Put(tube string, body []byte, params *PutParams) (uint64, error) {
	put := <-pool.putTokens
	id, err := put.Request(tube, body, params)
	pool.putTokens <- put

	return id, err
}
