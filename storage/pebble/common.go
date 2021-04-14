package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/dolthub/go-mysql-server/sql"
)

type storage struct {
	db *pebble.DB
}

func NewStorage(path string) (*storage, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &storage{
		db: db,
	}, nil
}

func (s *storage) create(key string) error {
	_, closer, err := s.db.Get([]byte(key))
	if err != nil && err != pebble.ErrNotFound {
		return sql.ErrTableAlreadyExists.New(key)
	}
	defer closer.Close()

	return s.db.Set([]byte(key), []byte{}, pebble.Sync)
}

func (s *storage) list(key string) error {
	// TODO: how to list?
	return nil
}
