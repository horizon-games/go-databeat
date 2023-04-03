package databeat

import (
	"crypto/sha256"
	"fmt"
	"net/http"
)

func SessionID(seed string, r ...*http.Request) string {
	if len(r) > 0 {
		return GenSessionIDFromRequest(r[0], seed)
	} else {
		return GenSessionID(seed)
	}
}

func GenSessionID(seed string, sessionOptions ...SessionOptions) string {
	opts := DefaultSessionOptions
	if len(sessionOptions) > 0 {
		opts = sessionOptions[0]
	}
	if !opts.Hash {
		return seed
	}

	return sha256Hex(seed)[0:50]
}

func GenSessionIDFromRequest(r *http.Request, seed string, sessionOptions ...SessionOptions) string {
	opts := DefaultSessionOptions
	if len(sessionOptions) > 0 {
		opts = sessionOptions[0]
	}
	if !opts.Hash {
		return seed
	}

	if opts.AgentSalt {
		userAgent := r.Header.Get("User-Agent")
		seed = fmt.Sprintf("%s:%s", seed, userAgent)
	}

	return GenSessionID(seed, sessionOptions...)
}

type SessionOptions struct {
	Hash      bool
	AgentSalt bool
}

var DefaultSessionOptions = SessionOptions{
	Hash: true, AgentSalt: true,
}

func sha256Hex(in string) string {
	return fmt.Sprintf("%s", sha256.Sum256([]byte(in)))
}
