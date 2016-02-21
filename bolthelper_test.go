package bh

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/golangplus/bytes"
	"github.com/golangplus/testing/assert"
)

func open(t *testing.T, path string) DB {
	fn := filepath.Join(os.TempDir(), path)
	os.RemoveAll(fn)
	db, err := Open(fn, 0644, nil)
	assert.NoErrorOrDie(t, err)
	return db
}

type Da struct {
	S string
}

func init() {
	gob.Register(Da{})
}

func TestGobReadWrite(t *testing.T) {
	db := open(t, "TestGobReadWrite")
	defer db.Close()

	k := [][]byte{[]byte("a"), []byte("b")}

	// Testing Tx.PutGob and Tx.GobValue
	assert.NoErrorOrDie(t, db.Update(func(tx Tx) error {
		return tx.PutGob(k, &Da{S: "hello"})
	}))
	found1 := false
	assert.NoError(t, db.View(func(tx Tx) error {
		return tx.GobValue(k, func(v interface{}) error {
			found1 = true
			da, ok := v.(Da)
			assert.True(t, "ok", ok)
			assert.Equal(t, "da", da, Da{S: "hello"})
			return nil
		})
	}))
	assert.True(t, "found1", found1)
	found2 := false
	assert.NoError(t, db.View(func(tx Tx) error {
		return tx.ForEachGob(k[:len(k)-1], func(_ Bucket, k bytesp.Slice, v interface{}) error {
			found2 = true
			da, ok := v.(Da)
			assert.True(t, "ok", ok)
			assert.Equal(t, "da", da, Da{S: "hello"})
			return nil
		})
	}))
	assert.True(t, "found2", found2)

	// Testing Bucket.PutGob, Bucket.GobValue, Bucket.ForEachGob
	bk := [][]byte{[]byte("bbb")}
	assert.NoErrorOrDie(t, db.Update(func(tx Tx) error {
		b, err := tx.CreateBucketIfNotExists(bk)
		assert.NoErrorOrDie(t, err)
		return b.PutGob(k, &Da{S: "world"})
	}))
	found3 := false
	assert.NoError(t, db.View(func(tx Tx) error {
		return tx.Bucket(bk, func(b Bucket) error {
			return b.GobValue(k, func(v interface{}) error {
				found3 = true
				da, ok := v.(Da)
				assert.True(t, "ok", ok)
				assert.Equal(t, "da", da, Da{S: "world"})
				return nil
			})
		})
	}))
	assert.True(t, "found3", found3)
	found4 := false
	assert.NoError(t, db.View(func(tx Tx) error {
		return tx.Bucket(bk, func(b Bucket) error {
			return b.ForEachGob(k[:len(k)-1], func(k bytesp.Slice, v interface{}) error {
				found4 = true
				da, ok := v.(Da)
				assert.True(t, "ok", ok)
				assert.Equal(t, "da", da, Da{S: "world"})
				return nil
			})
		})
	}))
	assert.True(t, "found4", found4)
}
