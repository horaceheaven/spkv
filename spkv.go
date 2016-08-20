package spkv

import (
	"github.com/boltdb/bolt"
	"errors"
	"time"
)

type KVStore struct {
	db *bolt.DB
}

var (
	ErrNotFound = errors.New("spkv: key not found")
	ErrBadValue = errors.New("spkv: bad value")

	bucketName = []byte("kv")
)

func Open(path string) (*KVStore, error) {
	opts := &bolt.Options{
		Timeout: 50 * time.Millisecond,
	}

	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		})

		if err != nil {
			return nil, err
		} else {
			return &KVStore{db: db}, nil
		}
	}
}
