package beanstalk

import "sync"

// ConsumerPool maintains a pool of beanstalk consumers.
type ConsumerPool struct {
	// C offers up newly reserved beanstalk jobs.
	C         <-chan *Job
	c         chan *Job
	consumers []*Consumer
	sync.Mutex
}

// NewConsumerPool creates a pool of beanstalk consumers.
func NewConsumerPool(urls []string, tubes []string, options *Options) (*ConsumerPool, error) {
	jobC := make(chan *Job)
	pool := &ConsumerPool{C: jobC, c: jobC}

	// Create a consumer for each URL.
	for _, url := range urls {
		consumer, err := NewConsumer(url, tubes, jobC, options)
		if err != nil {
			return nil, err
		}

		pool.consumers = append(pool.consumers, consumer)
	}

	// Start all the consumers.
	for _, consumer := range pool.consumers {
		consumer.Start()
	}

	return pool, nil
}

// Stop shuts down all the consumers in the pool.
func (pool *ConsumerPool) Stop() {
	pool.Lock()
	defer pool.Unlock()

	for i, consumer := range pool.consumers {
		consumer.Stop()
		pool.consumers[i] = nil
	}
	pool.consumers = []*Consumer{}
}

// Play tells all the consumers to start reservering jobs.
func (pool *ConsumerPool) Play() {
	pool.Lock()
	defer pool.Unlock()

	for _, consumer := range pool.consumers {
		consumer.Play()
	}
}

// Pause tells all the consumer to stop reservering jobs.
func (pool *ConsumerPool) Pause() {
	pool.Lock()
	defer pool.Unlock()

	for _, consumer := range pool.consumers {
		consumer.Pause()
	}
}
