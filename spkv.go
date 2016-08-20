package spkv

import (
	"github.com/boltdb/bolt"
	"errors"
	"time"
	"bytes"
	"encoding/gob"
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

func (kvs *KVStore) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}

	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return nil
	}

	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
	})
}

func (kvs *KVStore) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()

		if k, v := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else if value == nil {
			return nil
		} else {
			decoder := gob.NewDecoder(bytes.NewReader(v))
			return decoder.Decode(value)
		}
	})
}

func (kvs *KVStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()
		if k, _ := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return cursor.Delete()
		}
	})
}

func (kvs *KVStore) Close() error {
	return kvs.db.Close()
}
