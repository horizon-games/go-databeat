package databeat

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"net/http"
)

type Ident uint8

const (
	IDENT_ANON Ident = iota
	IDENT_PRIVATE
	IDENT_USER
)

func UserID(userID string, privacyOptions PrivacyOptions, r ...*http.Request) (string, Ident) {
	if len(r) > 0 {
		return GenUserIDFromRequest(r[0], userID, privacyOptions)
	} else {
		return GenUserID(userID, privacyOptions)
	}
}

func GenUserID(userID string, privacyOptions PrivacyOptions) (string, Ident) {
	if userID == "" {
		return fmt.Sprintf("%d", mrand.Int63n(100000000000000)), IDENT_ANON
	}
	if !privacyOptions.UserIDHash {
		return userID, IDENT_USER
	}
	if privacyOptions.ExtraSalt != "" {
		userID = fmt.Sprintf("%s:%s", userID, privacyOptions.ExtraSalt)
	}

	return sha256Hex(userID)[0:50], IDENT_PRIVATE
}

func GenUserIDFromRequest(r *http.Request, userID string, privacyOptions PrivacyOptions) (string, Ident) {
	if userID == "" {
		// TODO: if we wanted, we could do IP + UA hash here and use IDENT_PRIVATE
		return fmt.Sprintf("%d", mrand.Int63n(100000000000000)), IDENT_ANON
	}
	if !privacyOptions.UserIDHash {
		return userID, IDENT_USER
	}

	if r != nil && privacyOptions.UserAgentSalt && r.Header.Get("User-Agent") != "" {
		userAgent := r.Header.Get("User-Agent")
		userID = fmt.Sprintf("%s:%s", userID, userAgent)
	}
	if privacyOptions.ExtraSalt != "" {
		userID = fmt.Sprintf("%s:%s", userID, privacyOptions.ExtraSalt)
	}

	return GenUserID(userID, privacyOptions)
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
	ExtraSalt     string
}

var DefaultPrivacyOptions = PrivacyOptions{
	UserIDHash: true, UserAgentSalt: false, ExtraSalt: "_dbeat",
}

func sha256Hex(in string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(in)))
}
