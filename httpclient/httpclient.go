/*
github.com/mreiferson/go-httpclient

Provides an HTTP Transport that implements the `RoundTripper` interface and
can be used as a built in replacement for the standard library's, providing:

	* connection timeouts
	* request timeouts

This is a thin wrapper around `http.Transport` that sets dial timeouts and uses
Go's internal timer scheduler to call the Go 1.1+ `CancelRequest()` API.
*/
package httpclient

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ** NEW TYPES

// Transport provides a thin wrapper arounf http.Transport
type Transport struct {
	// Proxy specifies a function to return a proxy for a given
	// *http.Request. If the function returns a non-nil error, the
	// request is aborted with the provided error.
	// If Proxy is nil or returns a nil *url.URL, no proxy is used.
	Proxy func(*http.Request) (*url.URL, error)

	// TLSClientConfig specifies the TLS configuration to use with
	// tls.Client. If nil, the default configuration is used.
	TLSClientConfig *tls.Config

	// DisableKeepAlives, if true, prevents re-use of TCP connections
	// between different HTTP requests.
	DisableKeepAlives bool

	// DisableCompression, if true, prevents the Transport from
	// requesting compression with an "Accept-Encoding: gzip"
	// request header when the Request contains no existing
	// Accept-Encoding value. If the Transport requests gzip on
	// its own and gets a gzipped response, it's transparently
	// decoded in the Response.Body. However, if the user
	// explicitly requested gzip it is not automatically
	// uncompressed.
	DisableCompression bool

	// MaxIdleConnsPerHost, if non-zero, controls the maximum idle
	// (keep-alive) to keep per-host.  If zero,
	// http.DefaultMaxIdleConnsPerHost is used.
	MaxIdleConnsPerHost int

	// ConnectTimeout, if non-zero, is the maximum amount of time a dial will wait for
	// a connect to complete.
	ConnectTimeout time.Duration

	// ResponseHeaderTimeout, if non-zero, specifies the amount of
	// time to wait for a server's response headers after fully
	// writing the request (including its body, if any). This
	// time does not include the time to read the response body.
	ResponseHeaderTimeout time.Duration

	// RequestTimeout, if non-zero, specifies the amount of time for the entire
	// request to complete (including all of the above timeouts + entire response body).
	// This should never be less than the sum total of the above two timeouts.
	RequestTimeout time.Duration

	starter   sync.Once
	transport *http.Transport
}

// bodyCloseInterceptor
type bodyCloseInterceptor struct {
	io.ReadCloser
	timer *time.Timer
}

// ** PACKAGE VARIABLES

// Maintains a single Transport for all calls
var ClientTransport *Transport

//** INIT

// init is called to initialize the package with timeouts
func init() {
	ClientTransport = &Transport{
		ConnectTimeout:        10 * time.Second,
		RequestTimeout:        70 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
}

//** PUBLIC FUNCTIONS

// Version returns the current version of the package
func Version() string {
	return "0.4.1"
}

// Get implements an http get with timeouts
func Get(url string) (resp *http.Response, err error) {
	client := &http.Client{Transport: ClientTransport}
	req, _ := http.NewRequest("GET", url, nil)
	resp, err = client.Do(req)
	if err != nil {
		return resp, err
	}

	return resp, err
}

// DoRequest implements a client do with timeouts
func DoRequest(req *http.Request) (resp *http.Response, err error) {
	client := &http.Client{Transport: ClientTransport}
	resp, err = client.Do(req)
	if err != nil {
		return resp, err
	}

	return resp, err
}

// Close cleans up the Transport, currently a no-op
func (t *Transport) Close() error {
	ClientTransport.Close()
	return nil
}

//** TRANSPORT MEMBER FUNCTIONS

// lazyStart
func (t *Transport) lazyStart() {
	dialer := &net.Dialer{Timeout: t.ConnectTimeout}
	t.transport = &http.Transport{
		Dial:                  dialer.Dial,
		Proxy:                 t.Proxy,
		TLSClientConfig:       t.TLSClientConfig,
		DisableKeepAlives:     t.DisableKeepAlives,
		DisableCompression:    t.DisableCompression,
		MaxIdleConnsPerHost:   t.MaxIdleConnsPerHost,
		ResponseHeaderTimeout: t.ResponseHeaderTimeout,
	}
}

// RoundTrip implements the RoundTripper interface
//  req: The http request to process
func (t *Transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	t.starter.Do(t.lazyStart)

	if t.RequestTimeout > 0 {
		timer := time.AfterFunc(t.RequestTimeout, func() {
			t.transport.CancelRequest(req)
		})

		resp, err = t.transport.RoundTrip(req)
		if err != nil {
			timer.Stop()
		} else {
			resp.Body = &bodyCloseInterceptor{ReadCloser: resp.Body, timer: timer}
		}
	} else {
		resp, err = t.transport.RoundTrip(req)
	}

	return
}

//** INTERCEPTOR MEMBER FUNCTIONS

// Close
func (bci *bodyCloseInterceptor) Close() error {
	bci.timer.Stop()
	return bci.ReadCloser.Close()
}
