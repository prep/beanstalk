package beanstalk

// Response defines the response of a Put request.
type Response struct {
	ID    uint64
	Error error
}
