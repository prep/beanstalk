package beanstalk

import "testing"

func TestValidURIs(t *testing.T) {
	cases := map[string]struct {
		uris     []string
		expected bool
	}{
		"no address": {
			uris:     []string{},
			expected: false,
		},
		"unix socket": {
			uris:     []string{"unix:///tmp/beanstalk.sock"},
			expected: true,
		},
		"tcp socket": {
			uris:     []string{"tls://localhost:12345"},
			expected: true,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			actual := true
			if err := ValidURIs(c.uris); err != nil {
				actual = false
			}

			if actual != c.expected {
				t.Errorf("Got validity: %v, expected: %v", actual, c.expected)
			}
		})
	}
}

func TestParseURI(t *testing.T) {
	t.Run("WithValidSchemes", func(t *testing.T) {
		cases := []struct {
			uri     string
			uriType uriType
			address string
		}{
			{
				uri:     "beanstalk://localhost:12345",
				uriType: uriTCPType,
				address: "localhost:12345",
			},
			{
				uri:     "beanstalks://localhost:12345",
				uriType: uriTLSType,
				address: "localhost:12345",
			},
			{
				uri:     "tls://localhost:12345",
				uriType: uriTLSType,
				address: "localhost:12345",
			},
			{
				uri:     "unix:///tmp/beanstalkd.sock",
				uriType: uriUDSType,
				address: "/tmp/beanstalkd.sock",
			},
		}

		for _, c := range cases {
			t.Run(string(c.uriType), func(t *testing.T) {
				address, uriType, err := parseURI(c.uri)
				if err != nil {
					t.Errorf("Unable to parse URI: %s", c.uri)
				}

				if address != c.address {
					t.Errorf("Got address: %q, expected: %q", address, c.address)
				}

				if uriType != c.uriType {
					t.Errorf("Got URI type: %q, expected: %q", uriType, c.uriType)
				}
			})
		}
	})

	t.Run("WithMissingScheme", func(t *testing.T) {
		host, uriType, err := parseURI("localhost:11300")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without scheme: %s", err)
		case host != "localhost:11300":
			t.Errorf("Unexpected host: %s", host)
		case uriType != uriTCPType:
			t.Errorf("Got uri type: %q, expected: %q", uriType, uriTCPType)
		}
	})

	t.Run("WithMissingPort", func(t *testing.T) {
		host, _, err := parseURI("beanstalk://localhost")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without port")
		case host != "localhost:11300":
			t.Errorf("%s: Expected port 11300 to be added to the socket", host)
		}
	})

	t.Run("WithMissingTLSPort", func(t *testing.T) {
		host, _, err := parseURI("beanstalks://localhost")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without port")
		case host != "localhost:11400":
			t.Errorf("%s: Expected port 11400 to be added to the socket", host)
		}
	})

	t.Run("WithInvalidScheme", func(t *testing.T) {
		if _, _, err := parseURI("foo://localhost:12345"); err == nil {
			t.Fatal("Expected an error, but got nothing")
		}
	})
}

func TestMultiply(t *testing.T) {
	list := multiply([]string{"a", "b", "c"}, 3)
	if len(list) != 9 {
		t.Fatalf("Expected 9 items in the list")
	}

	for i, item := range list {
		switch i % 3 {
		case 0:
			if item != "a" {
				t.Fatalf("Expected a for item %d", i)
			}
		case 1:
			if item != "b" {
				t.Fatalf("Expected b for item %d", i)
			}
		case 2:
			if item != "c" {
				t.Fatalf("Expected c for item %d", i)
			}
		}
	}
}
