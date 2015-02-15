package beanstalk

// ConsumerPool maintains a pool of Consumer objects.
type ConsumerPool struct {
	C         chan *Job
	consumers []*Consumer
}

// NewConsumerPool creates a pool of Consumer objects.
func NewConsumerPool(sockets []string, tubes []string, options *Options) *ConsumerPool {
	pool := &ConsumerPool{C: make(chan *Job)}

	for _, socket := range sockets {
		pool.consumers = append(pool.consumers, NewConsumer(socket, tubes, pool.C, options))
	}

	return pool
}

// Stop shuts down all the consumers in the pool.
func (pool *ConsumerPool) Stop() {
	for i, consumer := range pool.consumers {
		consumer.Stop()
		pool.consumers[i] = nil
	}
}

// Play tells all the consumers to start reservering jobs.
func (pool *ConsumerPool) Play() {
	for _, consumer := range pool.consumers {
		consumer.Play()
	}
}

// Pause tells all the consumer to stop reservering jobs.
func (pool *ConsumerPool) Pause() {
	for _, consumer := range pool.consumers {
		consumer.Pause()
	}
}
