package bh

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/golangplus/testing/assert"
)

func TestRefCountBox_Basic(t *testing.T) {
	fn := path.Join(os.TempDir(), "TestRefCountBox.bolt")
	assert.NoError(t, os.RemoveAll(fn))

	b := RefCountBox{DataPath: fn}
	db, err := b.Alloc()
	assert.NoError(t, err)
	assert.ValueShould(t, "db.DB", db.DB, db.DB != nil, "is nil")
	assert.Equal(t, "b.count", b.count, 1)
	assert.ValueShould(t, "b.db.DB", b.db.DB, b.db.DB != nil, "is nil")

	b.Free()
	assert.Equal(t, "b.count", b.count, 0)
	assert.ValueShould(t, "b.db.DB", b.db.DB, b.db.DB == nil, "is not nil")
}

func TestRefCountBox_OpenFunc(t *testing.T) {
	fn := path.Join(os.TempDir(), "TestRefCountBox.bolt")
	assert.NoError(t, os.RemoveAll(fn))

	openFuncCalled := false

	b := RefCountBox{
		DataPath: fn,
		OpenFunc: func(path string) (DB, error) {
			openFuncCalled = true
			return Open(path, 0644, nil)
		},
	}
	db, err := b.Alloc()
	assert.NoError(t, err)
	assert.ValueShould(t, "db.DB", db.DB, db.DB != nil, "is nil")
	assert.Equal(t, "b.count", b.count, 1)
	assert.ValueShould(t, "b.db.DB", b.db.DB, b.db.DB != nil, "is nil")
	assert.True(t, "openFuncCalled", openFuncCalled)

	b.Free()
	assert.Equal(t, "b.count", b.count, 0)
	assert.ValueShould(t, "b.db.DB", b.db.DB, b.db.DB == nil, "is not nil")
}

func TestRefCountBox_OpenFuncFailed(t *testing.T) {
	fn := path.Join(os.TempDir(), "TestRefCountBox.bolt")
	assert.NoError(t, os.RemoveAll(fn))

	openFuncCalled := false
	failedErr := errors.New("failed")

	b := RefCountBox{
		DataPath: fn,
		OpenFunc: func(path string) (DB, error) {
			openFuncCalled = true
			return DB{}, failedErr
		},
	}
	db, err := b.Alloc()
	assert.Equal(t, "err", err, failedErr)
	assert.ValueShould(t, "db.DB", db.DB, db.DB == nil, "is not nil")
	assert.ValueShould(t, "b.db.DB", b.db.DB, b.db.DB == nil, "is not nil")
	assert.Equal(t, "b.count", b.count, 0)
	assert.True(t, "openFuncCalled", openFuncCalled)
}
