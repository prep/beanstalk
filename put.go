package beanstalk

import "time"

// Put describes a put request.
type Put struct {
	Body     []byte
	Tube     string
	Params   *PutParams
	Response chan PutResponse
}

// PutParams describe the parameters for a put request.
type PutParams struct {
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}

// PutResponse defines the response of a Put request.
type PutResponse struct {
	ID    uint64
	Error error
}
