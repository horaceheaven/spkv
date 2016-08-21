package spkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"os"
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

const (
	logPrefix       = "spkv "
	DefaultTimeout  = 50 * time.Millisecond
	DefaultPath     = "spkv.db"
	DefaultFileMode = 0640
)

type Opts struct {
	Timeout  time.Duration
	Path     string
	FileMode os.FileMode
	Debug    bool
}

func Open(options Opts) (*KVStore, error) {
	timeOut := options.Timeout
	path := options.Path
	fileMode := options.FileMode

	log.SetPrefix(logPrefix)

	if !options.Debug {
		log.SetOutput(ioutil.Discard)
	}

	if options.Timeout <= 0 {
		timeOut = DefaultTimeout
		log.Print("using defult timeout of ", DefaultTimeout)
	}

	if options.Path == "" {
		path = DefaultPath
		log.Print("using default path of ", DefaultPath)
	}

	if options.FileMode <= 0 {
		fileMode = DefaultFileMode
		log.Printf("using default default file mode of %o", DefaultFileMode)
	}

	opts := &bolt.Options{
		Timeout: timeOut,
	}

	if store, err := bolt.Open(path, fileMode, opts); err != nil {
		log.Print(err)
		return nil, err
	} else {
		err := store.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		})

		if err != nil {
			log.Print(err)
			return nil, err
		} else {
			log.Print("created store")
			return &KVStore{db: store}, nil
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
		log.Printf("PUT: key=%s, value=%s", key, value)
		return tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
	})
}

func (kvs *KVStore) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()

		if k, v := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			log.Print("GET: not found")
			return ErrNotFound
		} else if value == nil {
			log.Print("GET: nil value")
			return nil
		} else {
			log.Print("GET: getting value for key=", key)
			decoder := gob.NewDecoder(bytes.NewReader(v))
			return decoder.Decode(value);
		}
	})
}

func (kvs *KVStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()
		if k, _ := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			log.Print("DELETE: not found")
			return ErrNotFound
		} else {
			log.Print("DELETE: deleting key=", key)
			return cursor.Delete()
		}
	})
}

func (kvs *KVStore) Close() error {
	log.Print("closing store")
	return kvs.db.Close()
}
