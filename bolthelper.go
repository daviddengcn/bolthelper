// Package bh provides some helper classes for convenience of using github.com/boltdb/bolt package.
// All wrapper classes will use their friend wrapper classes whenever possible.
// All returned errors are with call stackes using github.com/golangplus/errors.
package bh

import (
	"io"
	"os"

	"github.com/golangplus/bytes"
	"github.com/golangplus/errors"

	"github.com/boltdb/bolt"
)

// A wrapper to *bolt.DB.
type DB struct {
	*bolt.DB
}

// A wrapper to *bolt.Tx.
type Tx struct {
	*bolt.Tx
}

// A wrapper to *bolt.Bucket.
type Bucket struct {
	*bolt.Bucket
}

// A wrapper to *bolt.Cursor.
type Cursor struct {
	*bolt.Cursor
}

// Open creates and opens a database at the given path. If the file does
// not exist then it will be created automatically. Passing in nil options
// will cause Bolt to open the database with the default options.
func Open(path string, mode os.FileMode, options *bolt.Options) (DB, error) {
	db, err := bolt.Open(path, mode, options)
	return DB{db}, errorsp.WithStacks(err)
}

// Batch wraps bolt.DB.Batch.
func (db DB) Batch(f func(Tx) error) error {
	return errorsp.WithStacks(db.DB.Batch(func(tx *bolt.Tx) error {
		return errorsp.WithStacks(f(Tx{tx}))
	}))
}

// Begin wraps bolt.DB.Begin.
func (db DB) Begin(writable bool) (Tx, error) {
	tx, err := db.DB.Begin(writable)
	return Tx{tx}, errorsp.WithStacks(err)
}

// Close releases all database resources. All transactions must be closed
// before closing the database.
func (db DB) Close() error {
	return errorsp.WithStacks(db.DB.Close())
}

// Sync wraps bolt.DB.Sync.
func (db DB) Sync() error {
	return errorsp.WithStacks(db.DB.Sync())
}

// Update wraps bolt.DB.Update.
func (db DB) Update(f func(Tx) error) error {
	return errorsp.WithStacks(db.DB.Update(func(tx *bolt.Tx) error {
		return errorsp.WithStacks(f(Tx{tx}))
	}))
}

// View wraps bolt.DB.View.
func (db DB) View(f func(Tx) error) error {
	return errorsp.WithStacks(db.DB.View(func(tx *bolt.Tx) error {
		return errorsp.WithStacks(f(Tx{tx}))
	}))
}

func (tx Tx) Bucket(folders [][]byte, f func(Bucket) error) error {
	b := tx.Tx.Bucket(folders[0])
	if b == nil {
		return nil
	}
	if len(folders) == 1 {
		return errorsp.WithStacks(f(Bucket{b}))
	}
	return Bucket{b}.OpenBucket(folders[1:], f)
}

// Commit writes all changes to disk and updates the meta page. Returns an
// error if a disk write error occurs, or if Commit is called on a
// read-only transaction.
func (tx Tx) Commit() error {
	return errorsp.WithStacks(tx.Tx.Commit())
}

// CopyFile copies the entire database to file at the given path. A reader
// transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (tx Tx) CopyFile(path string, mode os.FileMode) error {
	return errorsp.WithStacks(tx.Tx.CopyFile(path, mode))
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already
// exist. Returns an error if the bucket name is blank, or if the bucket
// name is too long. The bucket instance is only valid for the lifetime of
// the transaction.
func (tx Tx) CreateBucketIfNotExists(folders [][]byte) (Bucket, error) {
	b, err := tx.Tx.CreateBucketIfNotExists(folders[0])
	if err != nil {
		return Bucket{}, nil
	}
	if len(folders) == 1 {
		return Bucket{b}, nil
	}
	return Bucket{b}.CreateBucketIfNotExists(folders[1:])
}

func (tx Tx) Cursor(folders [][]byte, f func(Cursor) error) error {
	if len(folders) == 0 {
		return errorsp.WithStacks(f(Cursor{tx.Tx.Cursor()}))
	}
	return tx.Bucket(folders, func(b Bucket) error {
		return errorsp.WithStacks(f(b.Cursor()))
	})
}

// DB returns a reference to the database that created the transaction.
func (tx Tx) DB() DB {
	return DB{tx.Tx.DB()}
}

// Delete deletes a key.
func (tx Tx) Delete(k [][]byte) error {
	if len(k) == 1 {
		return errorsp.WithStacks(tx.Tx.DeleteBucket(k[0]))
	}
	return tx.Bucket(k[:len(k)-1], func(b Bucket) error {
		return errorsp.WithStacks(b.Bucket.Delete(k[len(k)-1]))
	})
}

// ForEach iterates over all key values of a folder.
func (tx Tx) ForEach(folders [][]byte, f func(Bucket, bytesp.Slice, bytesp.Slice) error) error {
	return tx.Bucket(folders, func(b Bucket) error {
		return errorsp.WithStacks(b.Bucket.ForEach(func(k, v []byte) error {
			return errorsp.WithStacks(f(b, bytesp.Slice(k), bytesp.Slice(v)))
		}))
	})
}

// Rollback closes the transaction and ignores all previous updates.
// Read-only transactions must be rolled back and not committed.
func (tx Tx) Rollback() error {
	return errorsp.WithStacks(tx.Tx.Rollback())
}

// WriteTo writes the entire database to a writer. If err == nil then
// exactly tx.Size() bytes will be written into the writer.
func (tx *Tx) WriteTo(w io.Writer) (int64, error) {
	n, err := tx.Tx.WriteTo(w)
	return n, errorsp.WithStacks(err)
}

// Put sets the value for a key in the transaction.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction,
// if the key is blank, if the key is too large, or if the value is too
// large.
func (tx Tx) Put(k [][]byte, v []byte) error {
	b, err := tx.CreateBucketIfNotExists(k[:len(k)-1])
	if err != nil {
		return err
	}
	return errorsp.WithStacks(b.Bucket.Put(k[len(k)-1], v))
}

// Value tries to get a value from the transaction. If the key does not
// exist, the f is not called and nil is return.
func (tx Tx) Value(k [][]byte, f func(v bytesp.Slice) error) error {
	return tx.Bucket(k[:len(k)-1], func(b Bucket) error {
		v := b.Bucket.Get(k[len(k)-1])
		if v == nil {
			return nil
		}
		return errorsp.WithStacks(f(bytesp.Slice(v)))
	})
}

// Updates fetches the current value and updates to a new value. If a nil
// value is returned by f, the item is deleted.
func (tx Tx) Update(k [][]byte, f func(bytesp.Slice) (bytesp.Slice, error)) error {
	b, err := tx.CreateBucketIfNotExists(k[:len(k)-1])
	if err != nil {
		return err
	}
	v, err := f(b.Bucket.Get(k[len(k)-1]))
	if err != nil {
		return errorsp.WithStacks(err)
	}
	if v == nil {
		return errorsp.WithStacks(b.Bucket.Delete(k[len(k)-1]))
	}
	return errorsp.WithStacks(b.Bucket.Put(k[len(k)-1], v))
}

// Bucket retrieves a nested bucket by name. Returns nil if the bucket
// does not exist. The bucket instance is only valid for the lifetime of
// the transaction.
func (b Bucket) OpenBucket(folders [][]byte, f func(Bucket) error) error {
	bb := b.Bucket
	for _, fld := range folders {
		bb = bb.Bucket(fld)
		if bb == nil {
			return nil
		}
	}
	return errorsp.WithStacks(f(Bucket{bb}))
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already
// exist and returns a reference to it. Returns an error if the bucket
// name is blank, or if the bucket name is too long. The bucket instance
// is only valid for the lifetime of the transaction.
func (b Bucket) CreateBucketIfNotExists(folders [][]byte) (Bucket, error) {
	bb := b.Bucket
	for _, fld := range folders {
		var err error
		if bb, err = bb.CreateBucketIfNotExists(fld); err != nil {
			return Bucket{}, errorsp.WithStacks(err)
		}
	}
	return Bucket{bb}, nil
}

// Cursor creates a cursor associated with the bucket. The cursor is only
// valid as long as the transaction is open. Do not use a cursor after the
// transaction is closed.
func (b Bucket) Cursor() Cursor {
	return Cursor{b.Bucket.Cursor()}
}

// Delete removes a key from the bucket. If the key does not exist then
// nothing is done and a nil error is returned. Returns an error if the
// bucket was created from a read-only transaction.
func (b Bucket) Delete(k [][]byte) error {
	return b.OpenBucket(k[:len(k)-1], func(b Bucket) error {
		return errorsp.WithStacks(b.Bucket.Delete(k[len(k)-1]))
	})
}

// ForEach executes a function for each key/value pair in a bucket. If the
// provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not
// modify the bucket; this will result in undefined behavior.
func (b Bucket) ForEach(folders [][]byte, f func(k, v bytesp.Slice) error) error {
	return b.OpenBucket(folders, func(b Bucket) error {
		return errorsp.WithStacks(b.Bucket.ForEach(func(k, v []byte) error {
			return errorsp.WithStacks(f(bytesp.Slice(k), bytesp.Slice(k)))
		}))
	})
}

// Get retrieves the value for a key in the bucket. f is not called if
// the key does not exist or if the key is a nested bucket.
func (b Bucket) Value(k [][]byte, f func(bytesp.Slice) error) error {
	return b.OpenBucket(k[:len(k)-1], func(b Bucket) error {
		if v := b.Bucket.Get(k[len(k)-1]); v != nil {
			return errorsp.WithStacks(f(bytesp.Slice(v)))
		}
		return nil
	})
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b Bucket) NextSequence() (uint64, error) {
	s, err := b.Bucket.NextSequence()
	return s, errorsp.WithStacks(err)
}

// Put sets the value for a key in the bucket. If the key exist then its
// previous value will be overwritten. Supplied value must remain valid
// for the life of the transaction. Returns an error if the bucket was
// created from a read-only transaction, if the key is blank, if the key
// is too large, or if the value is too large.
func (b Bucket) Put(k [][]byte, v []byte) error {
	return b.OpenBucket(k[:len(k)-1], func(b Bucket) error {
		return errorsp.WithStacks(b.Bucket.Put(k[len(k)-1], v))
	})
}

// Tx returns the tx of the bucket.
func (b Bucket) Tx() Tx {
	return Tx{b.Bucket.Tx()}
}

// Bucket returns the bucket that this cursor was created from.
func (c Cursor) Bucket() Bucket {
	return Bucket{c.Cursor.Bucket()}
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is
// not writable.
func (c Cursor) Delete() error {
	return errorsp.WithStacks(c.Cursor.Delete())
}
