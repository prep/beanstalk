package beanstalk

import "time"

// Put defines a beanstalk put request and response. It is assumed that an
// object of this type is shared between a goroutine that sets the request
// and a goroutine that sets the response.
type Put struct {
	request  PutRequest
	response PutResponse
	putC     chan<- *Put
	respC    chan struct{}
}

// NewPut returns a new Put object that operates on the specified producer
// channel.
func NewPut(putC chan<- *Put) *Put {
	return &Put{putC: putC, respC: make(chan struct{})}
}

// SendRequest sends a put request to a producer.
func (put *Put) SendRequest(tube string, body []byte, params *PutParams) {
	put.request.Tube = tube
	put.request.Body = body
	put.request.Params = params
	put.putC <- put
}

// SendResponse sends a put response back from a producer.
func (put *Put) SendResponse(ID uint64, err error) {
	put.response.ID, put.response.Err = ID, err
	put.respC <- struct{}{}
}

// ReceiveResponse returns parameters set by the producer.
func (put *Put) ReceiveResponse() (uint64, error) {
	<-put.respC
	return put.response.ID, put.response.Err
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
