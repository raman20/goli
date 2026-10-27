package server

import "crypto/sha256"

func sum(b []byte) []byte { h := sha256.Sum256(b); return h[:] }
