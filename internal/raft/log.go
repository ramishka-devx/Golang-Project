package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type LogEntry struct {
	Term    int
	Command any
}

type StableLog interface {
	Append(entries ...LogEntry) int // returns last index appended (1‑based)
	At(index int) (LogEntry, bool)  // returns false if index < firstIndex or > lastIndex
	LastIndexTerm() (int, int)
	LastIndex() int
	FirstIndex() int

	TruncateSuffix(idx int) error
	TruncateBefore(index int)
}

// ---------------- util ---------------------
func u64ToKey(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}
func keyToU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// -------------- BoltLog --------------------
type boltLog struct {
	db   *bolt.DB
	mu   sync.Mutex
	base uint64 // first index = base
}

func NewBoltLog(db *bolt.DB) StableLog {
	var base uint64 = 1
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("log"))
		if err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		if v := meta.Get([]byte("firstIndex")); v != nil {
			base = binary.BigEndian.Uint64(v)
		} else {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], base)
			err = meta.Put([]byte("firstIndex"), b[:])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err) // This is a critical initialization error
	}
	return &boltLog{db: db, base: base}
}

// --------------- StableLog interface -----------------------
func (l *boltLog) Append(entries ...LogEntry) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	var last uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		for _, e := range entries {
			last = uint64(l.LastIndex() + 1)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(e); err != nil {
				return err
			}
			if err := b.Put(u64ToKey(last), buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Return the last successful index or 0 if no entries were appended
		return int(last - 1)
	}
	return int(last)
}

func (l *boltLog) At(index int) (LogEntry, bool) {
	if index < int(l.base) {
		return LogEntry{}, false
	}

	var entry LogEntry
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if v := b.Get(u64ToKey(uint64(index))); v != nil {
			return gob.NewDecoder(bytes.NewReader(v)).Decode(&entry)
		}
		return nil
	})
	if err != nil {
		return LogEntry{}, false
	}
	return entry, true
}

func (l *boltLog) LastIndexTerm() (int, int) {
	var idx, term int
	_ = l.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("log")).Cursor()
		k, v := c.Last()
		if k == nil {
			idx, term = int(l.base-1), 0
			return nil
		}
		idx = int(keyToU64(k))
		var e LogEntry
		_ = gob.NewDecoder(bytes.NewReader(v)).Decode(&e)
		term = e.Term
		return nil
	})
	return idx, term
}

func (l *boltLog) LastIndex() int {
	var last uint64
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		if k, _ := c.Last(); k != nil {
			last = keyToU64(k)
		}
		return nil
	})
	if err != nil {
		return int(l.base - 1)
	}
	return int(last)
}

func (l *boltLog) FirstIndex() int {
	return int(l.base)
}

// ------------ snapshot compaction ---------------
func (l *boltLog) TruncateBefore(index int) {
	if index <= int(l.base) {
		return
	}

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil && keyToU64(k) < uint64(index); k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("meta"))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(index))
		return meta.Put([]byte("firstIndex"), buf[:])
	})
	if err != nil {
		// Log the error but continue
		fmt.Printf("Error truncating log: %v\n", err)
	}
	l.base = uint64(index)
}

func (l *boltLog) TruncateSuffix(idx int) error {
	if idx < int(l.base) {
		return fmt.Errorf("index %d is before first index %d", idx, l.base)
	}

	return l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		for k, _ := c.Seek(u64ToKey(uint64(idx + 1))); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}
