package raft

// StableStore holds the Raft persistent metadata.

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

type StableStore interface {
	Term() int
	SetTerm(t int) error
	VotedFor() string
	SetVotedFor(id string) error
	LastApplied() int
	SetLastApplied(index int) error
}

// ------------------------------------------------------------
// Bolt-backed implementation
// ------------------------------------------------------------
const (
	bMeta       = "meta" // bucket name
	kTerm       = "term"
	kVotedFor   = "voted_for"
	LastApplied = "last_applied"
)

type boltStore struct{ db *bolt.DB }

func NewBoltStore(db *bolt.DB) StableStore {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bMeta))
		return err
	}); err != nil {
		panic(err)
	}
	return &boltStore{db: db}
}

func (s *boltStore) Term() int {
	var t uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kTerm)); v != nil {
			t = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		// Return 0 as default term if there's an error
		return 0
	}
	return int(t)
}

func (s *boltStore) SetTerm(term int) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(term))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kTerm), buf[:])
	})
}

func (s *boltStore) VotedFor() string {
	var id string
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kVotedFor)); v != nil {
			id = string(v)
		}
		return nil
	})
	if err != nil {
		// Return empty string as default if there's an error
		return ""
	}
	return id
}

func (s *boltStore) SetVotedFor(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kVotedFor), []byte(id))
	})
}

func (s *boltStore) LastApplied() int {
	var last uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(LastApplied)); v != nil {
			last = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		// Return 0 as default if there's an error
		return 0
	}
	return int(last)
}

func (s *boltStore) SetLastApplied(index int) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(LastApplied), buf[:])
	})
}
