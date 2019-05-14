package beanstalk

import (
	"testing"
)

type urlTest struct {
	URL    string
	Socket string
	UseTLS bool
	Err    bool
}

var urls = []urlTest{
	{URL: "beanstalk://test.com", Socket: "test.com:11300", UseTLS: false, Err: false},
	{URL: "beanstalk://test.com:11300", Socket: "test.com:11300", UseTLS: false, Err: false},
	{URL: "beanstalks://test.com:10301", Socket: "test.com:10301", UseTLS: true, Err: false},
	{URL: "tls://test:1234", Socket: "test:1234", UseTLS: true, Err: false},
	{URL: "http://localhost:11300", Socket: "", UseTLS: false, Err: true},
	{URL: "localhost:11300", Socket: "localhost:11300", UseTLS: false, Err: false},
	{URL: "foobar", Socket: "foobar:11300", UseTLS: false, Err: false},
}

func TestParseURL(t *testing.T) {
	for _, url := range urls {
		socket, useTLS, err := ParseURL(url.URL)
		switch {
		case (err != nil) && !url.Err:
			t.Errorf("Expected URL %q to have no error, but got %#v", url.URL, err)
			continue
		case (err == nil) && url.Err:
			t.Errorf("Exepected URL %q to have an error, but got none", url.URL)
			continue
		}

		if socket != url.Socket {
			t.Errorf("Expected URL %q to have socket=%q, but got socket=%q", url.URL, url.Socket, socket)
		}
		if useTLS != url.UseTLS {
			t.Errorf("Expected URL %q to have a useTLS=%v, but got useTLS=%v", url.URL, url.UseTLS, useTLS)
		}
	}
}
