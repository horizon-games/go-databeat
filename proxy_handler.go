package databeat

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// ProxyHandler routes requests from /rpc/Databeat/* to the remote Databeat server.
func ProxyHandler(databeatHost string) func(next http.Handler) http.Handler {
	origin, err := url.Parse(databeatHost)

	// In case of an error with the host, we return a handler that does nothing.
	if err != nil {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r)
			})
		}
	}

	// Create a reverse proxy to the Databeat server.
	director := func(req *http.Request) {
		req.URL.Scheme = origin.Scheme
		req.URL.Host = origin.Host
		// NOTE: its unnecessary
		// req.URL.Path = singleJoiningSlash(origin.Path, req.URL.Path)
		req.Host = req.URL.Host
	}

	proxy := &httputil.ReverseProxy{Director: director}

	proxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Path does not match /rpc/Databeat/*, so we skip the proxy.
			if !strings.HasPrefix(r.URL.Path, "/rpc/Databeat/") {
				next.ServeHTTP(w, r)
				return
			}

			// Route request to the origin.
			proxy.ServeHTTP(w, r)
		})
	}
}

// singleJoiningSlash is copied from httputil.singleJoiningSlash method.
// func singleJoiningSlash(a, b string) string {
// 	aslash := strings.HasSuffix(a, "/")
// 	bslash := strings.HasPrefix(b, "/")
// 	switch {
// 	case aslash && bslash:
// 		return a + b[1:]
// 	case !aslash && !bslash:
// 		return a + "/" + b
// 	}
// 	return a + b
// }
