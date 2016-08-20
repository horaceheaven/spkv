package spkv

import (
	"testing"
	"os"
)

var (
	testDBName = "spkv-test.db"
)

func TestSPKVStore_Setup(t *testing.T) {
	os.Remove(testDBName)
}

func TestSPKVStore_Suite(t *testing.T) {
	store, err := Open(testDBName)

	if (err != nil) {
		t.Fatal(err)
	}

	if err := store.Put("key1", "somevalue"); err != nil {
		t.Fatal(err)
	}

	var value string

	if err := store.Get("key1", &value); err != nil {
		t.Fatal(err)
	} else if value != "somevalue" {
		t.Fatalf("got \"%s\", expected \"somevalue\"", value)
	}

	store.Close()
}

func TestSPKVStore_TearDown(t *testing.T) {
	os.Remove(testDBName)
}