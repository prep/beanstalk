package beanstalk

import "time"

// Put describes a put request.
type Put struct {
	Body      []byte
	Tube      string
	PutParams *PutParams
	Response  chan Response
}

// PutParams describe the parameters for a put request.
type PutParams struct {
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}
