package beanstalk

import "time"

// Put defines a beanstalk put request and response. It is assumed that an
// object of this type is shared between a goroutine that sets the request
// and a goroutine that sets the response.
type Put struct {
	request  PutRequest
	response PutResponse
	timer    *time.Timer
	options  *Options
	putC     chan<- *Put
	respC    chan struct{}
}

// NewPut returns a new Put object that operates on the specified producer
// channel.
func NewPut(putC chan<- *Put, options *Options) *Put {
	if options == nil {
		options = DefaultOptions()
	}

	timer := time.NewTimer(time.Second)
	timer.Stop()

	return &Put{
		timer:   timer,
		options: options,
		putC:    putC,
		respC:   make(chan struct{})}
}

// Request sends a put request to an available Producer. This function uses the
// ReadWriteTimeout field from Options{} to limit the time to wait for an
// available producer before it returns ErrNotConnected.
func (put *Put) Request(tube string, body []byte, params *PutParams) (ID uint64, err error) {
	put.request.Tube, put.request.Body, put.request.Params = tube, body, params
	if put.options.ReadWriteTimeout != 0 {
		put.timer.Reset(put.options.ReadWriteTimeout)
	}

	select {
	case put.putC <- put:
		put.timer.Stop()
	case <-put.timer.C:
		put.options.LogError("Unable to find a producer. Timeout was reached")
		return 0, ErrNotConnected
	}

	<-put.respC
	return put.response.ID, put.response.Err
}

// Response sends a put response back. This function is called from a producer.
func (put *Put) Response(ID uint64, err error) {
	put.response.ID, put.response.Err = ID, err
	put.respC <- struct{}{}
}

// PutRequest describes a put request.
type PutRequest struct {
	Body   []byte
	Tube   string
	Params *PutParams
}

// PutParams describe the parameters for a put request.
type PutParams struct {
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}

// PutResponse defines the response to a Put request.
type PutResponse struct {
	ID  uint64
	Err error
}
