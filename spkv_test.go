package spkv

import (
	"fmt"
	"os"
	"testing"
)

var (
	testDBName      = "spkv-test.db"
	benchMarkDBName = "spkv-bench.db"
)

func TestSPKVStore_Suite(t *testing.T) {
	os.Remove(testDBName)

	options := Opts{
		Path:    testDBName,
		Debug:   true,
	}

	store, err := Open(options)

	if err != nil {
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

	os.Remove(testDBName)
}

func BenchmarkSPKVStore_Put(b *testing.B) {
	os.Remove(benchMarkDBName)

	options := Opts{
		Path:    testDBName,
		Debug:   true,
	}

	store, err := Open(options)

	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.Put(fmt.Sprintf("key%d", i), "somevalue"); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	store.Close()
	os.Remove(benchMarkDBName)
}
