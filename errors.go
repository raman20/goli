// Package goli is a single-writer storage engine where one append-only log on
// object storage (or a local directory) is the only source of truth, and
// key-value, topics/consumer groups, documents, secondary indexes and vectors
// are "lenses": pure folds over that log.
package goli

import (
	"errors"

	"github.com/raman20/goli/format"
)

var (
	ErrNotFound       = errors.New("goli: not found")
	ErrConflict       = errors.New("goli: transaction conflict (serializability); retry")
	ErrFenced         = errors.New("goli: fenced: another writer took over this database")
	ErrUnknownOutcome = errors.New("goli: commit outcome unknown (storage error); reopen to learn whether it committed")
	ErrShredded       = errors.New("goli: subject has been crypto-shredded")
	ErrClosed         = errors.New("goli: database closed")
	ErrCorrupt        = errors.New("goli: corrupt log")
	ErrReadOnly       = errors.New("goli: read-only transaction")
	ErrBranchExists   = errors.New("goli: branch already exists")
	ErrNoBranch       = errors.New("goli: no such branch")
	ErrNotFastForward = errors.New("goli: promote is not a fast-forward")
	ErrBadLSN         = errors.New("goli: LSN out of range")
)

// Op re-exports the log op type for advanced users (Entries).
type Op = format.Op
