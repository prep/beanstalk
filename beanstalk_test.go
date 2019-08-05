package beanstalk

import "testing"

func TestParseURI(t *testing.T) {
	t.Run("WithValidSchemes", func(t *testing.T) {
		for _, scheme := range []string{"beanstalk", "beanstalks", "tls"} {
			uri := scheme + "://localhost:12345"

			host, useTLS, err := ParseURI(uri)
			switch {
			case err != nil:
				t.Errorf("Unable to parse URI: %s", uri)
			case host != "localhost:12345":
				t.Errorf("Unexpected host: %s", host)
			}

			switch scheme {
			case "beanstalk":
				if useTLS {
					t.Errorf("%s: scheme shouldn't support TLS", scheme)
				}
			case "beanstalks", "tls":
				if !useTLS {
					t.Errorf("%s: scheme should support TLS", scheme)
				}
			default:
				t.Fatalf("%s: unknown scheme", scheme)
			}
		}
	})

	t.Run("WithMissingScheme", func(t *testing.T) {
		host, useTLS, err := ParseURI("localhost:11300")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without scheme: %s", err)
		case host != "localhost:11300":
			t.Errorf("Unexpected host: %s", host)
		case useTLS:
			t.Error("Unexpected TLS to be set")
		}
	})

	t.Run("WithMissingPort", func(t *testing.T) {
		host, _, err := ParseURI("beanstalk://localhost")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without port")
		case host != "localhost:11300":
			t.Errorf("%s: Expected port 11300 to be added to the socket", host)
		}
	})

	t.Run("WithMissingTLSPort", func(t *testing.T) {
		host, _, err := ParseURI("beanstalks://localhost")
		switch {
		case err != nil:
			t.Fatalf("Error parsing URI without port")
		case host != "localhost:11400":
			t.Errorf("%s: Expected port 11400 to be added to the socket", host)
		}
	})

	t.Run("WithInvalidScheme", func(t *testing.T) {
		if _, _, err := ParseURI("foo://localhost:12345"); err == nil {
			t.Fatal("Expected an error, but got nothing")
		}
	})
}
