//go:build windows

package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

func newControlServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 16384}
}

// Decode the complete bounded control message before applying side effects.
// LimitReader alone accepts a valid prefix and silently ignores a second value.
func decodeControlJSON(w http.ResponseWriter, r *http.Request, max int64, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, max))
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("multiple control messages")
	}
	return nil
}
