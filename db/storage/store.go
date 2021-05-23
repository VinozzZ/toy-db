package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/dolthub/go-mysql-server/sql"
)

type Store struct {
	db *pebble.DB
}

func NewDefaultStorage() (*Store, error) {
	return NewStorage("/tmp/toy.db")
}

func NewStorage(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Create(key string) error {
	_, closer, err := s.db.Get([]byte(key))
	if err != nil && err != pebble.ErrNotFound {
		return sql.ErrTableAlreadyExists.New(key)
	}
	defer closer.Close()

	return s.db.Set([]byte(key), []byte{}, pebble.Sync)
}

func (s *Store) Interate(key string) error {
	// TODO: how to list?
	return nil
}
