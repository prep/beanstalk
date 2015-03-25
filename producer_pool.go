package beanstalk

import "time"

type putToken struct {
	Timer *time.Timer
	C     chan PutResponse
}

// ProducerPool maintains a pool of Producers with the purpose of spreading
// incoming Put requests over the maintained Producers.
type ProducerPool struct {
	putCh     chan *Put
	producers []*Producer
	putTokens chan *putToken
	options   Options
}

// NewProducerPool creates a pool of Producer objects.
func NewProducerPool(sockets []string, options Options) *ProducerPool {
	pool := &ProducerPool{putCh: make(chan *Put), options: SanitizeOptions(options)}
	pool.putTokens = make(chan *putToken, len(sockets)*2)

	for _, socket := range sockets {
		pool.producers = append(pool.producers, NewProducer(socket, pool.putCh, options))
	}

	// Create twice as many put-tokens as there are sockets.
	for i := 0; i < len(sockets)*2; i++ {
		timer := time.NewTimer(time.Second)
		timer.Stop()

		pool.putTokens <- &putToken{Timer: timer, C: make(chan PutResponse)}
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
	// Fetch the put token from the token queue.
	token := <-pool.putTokens

	// Create a Put object and set the request timer, if needed.
	put := &Put{Tube: tube, Body: body, Params: params, Response: token.C}
	if pool.options.ReadWriteTimeout != 0 {
		token.Timer.Reset(pool.options.ReadWriteTimeout)
	}

	select {
	// Hand the Put object over to an available producer.
	case pool.putCh <- put:
		token.Timer.Stop()

	// When the timeout hits, give back the put token and return NotConnectedErr.
	case <-token.Timer.C:
		pool.putTokens <- token
		return 0, ErrNotConnected
	}

	// Wait for the response and when it comes in, give back the response token.
	response := <-token.C
	pool.putTokens <- token

	return response.ID, response.Error
}
