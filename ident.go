package databeat

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
)

type Ident uint8

const (
	IDENT_ANON Ident = iota
	IDENT_PRIVATE
	IDENT_USER
)

func UserID(userID string, r ...*http.Request) (string, Ident) {
	if len(r) > 0 {
		return GenUserIDFromRequest(r[0], userID)
	} else {
		return GenUserID(userID)
	}
}

func GenUserID(userID string, privacyOptions ...PrivacyOptions) (string, Ident) {
	opts := DefaultPrivacyOptions
	if len(privacyOptions) > 0 {
		opts = privacyOptions[0]
	}
	if userID == "" {
		return "0", IDENT_ANON
	}
	if !opts.UserIDHash {
		return userID, IDENT_USER
	}

	return sha256Hex(userID)[0:50], IDENT_PRIVATE
}

func GenUserIDFromRequest(r *http.Request, userID string, privacyOptions ...PrivacyOptions) (string, Ident) {
	opts := DefaultPrivacyOptions
	if len(privacyOptions) > 0 {
		opts = privacyOptions[0]
	}
	if userID == "" {
		// TODO: if we wanted, we could do IP + UA hash here and use IDENT_PRIVATE
		return "0", IDENT_ANON
	}
	if !opts.UserIDHash {
		return userID, IDENT_USER
	}

	if opts.UserAgentSalt {
		userAgent := r.Header.Get("User-Agent")
		userID = fmt.Sprintf("%s:%s", userID, userAgent)
	}

	return GenUserID(userID, privacyOptions...)
}

func GenSessionID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return "0x" + hex.EncodeToString(bytes)
}

type PrivacyOptions struct {
	UserIDHash    bool
	UserAgentSalt bool
}

var DefaultPrivacyOptions = PrivacyOptions{
	UserIDHash: true, UserAgentSalt: true,
}

func sha256Hex(in string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(in)))
}
