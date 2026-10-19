// Package store wraps Pebble. Every batch commit is synced to disk before it returns.
package store

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Store is a durable ordered key-value store.
type Store struct {
	db      *pebble.DB
	once    sync.Once
	err     error
	failMu  sync.Mutex
	failErr error
}

// Batch is an atomic group of writes.
type Batch struct {
	b *pebble.Batch
	s *Store
}

// Open opens or creates a store in dir.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close flushes and closes the store.
func (s *Store) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}

// Get returns the value for key and whether it exists.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, true, nil
}

// NewBatch starts an atomic batch.
func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch(), s: s} }

// FailCommits makes every later commit fail with err, without writing (nil restores).
// It exists to test how callers handle disk failures.
func (s *Store) FailCommits(err error) {
	s.failMu.Lock()
	s.failErr = err
	s.failMu.Unlock()
}

// Set stages a write.
func (b *Batch) Set(k, v []byte) { _ = b.b.Set(k, v, nil) }

// Delete stages a delete.
func (b *Batch) Delete(k []byte) { _ = b.b.Delete(k, nil) }

// Empty reports whether the batch has no writes.
func (b *Batch) Empty() bool { return b.b.Empty() }

// Commit applies the batch and fsyncs before returning. Concurrent commits share fsyncs.
func (b *Batch) Commit() error {
	defer b.b.Close()
	b.s.failMu.Lock()
	err := b.s.failErr
	b.s.failMu.Unlock()
	if err != nil {
		return err
	}
	return b.b.Commit(pebble.Sync)
}

// Scan calls fn for every key with prefix, in key order.
func (s *Store) Scan(prefix []byte, fn func(k, v []byte) error) error {
	return s.ScanFrom(prefix, prefix, fn)
}

// ScanFrom calls fn for keys with prefix that are >= start, in key order.
func (s *Store) ScanFrom(start, prefix []byte, fn func(k, v []byte) error) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: upperBound(prefix)})
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			break
		}
		if err := fn(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...)); err != nil {
			return err
		}
	}
	return it.Error()
}

func upperBound(prefix []byte) []byte {
	ub := append([]byte(nil), prefix...)
	for i := len(ub) - 1; i >= 0; i-- {
		if ub[i] < 0xff {
			ub[i]++
			return ub[:i+1]
		}
	}
	return nil
}
