package bh

import (
	"sync"
)

// RefCountBox is a structure maintaining a reference-count guarded instance of DB.
type RefCountBox struct {
	sync.Mutex

	// The path to the bolt database file.
	DataPath string

	// Used to open a bolt DB. If not specified, bh.Open with 0644 mode and
	// default options will be used.
	OpenFunc func(path string) (DB, error)

	db    DB
	count int
}

// Alloc opens a DB if not openned yet. It adds a reference if already openned.
func (b *RefCountBox) Alloc() (DB, error) {
	b.Lock()
	defer b.Unlock()

	if b.db.DB == nil {
		var db DB
		var err error
		if b.OpenFunc == nil {
			db, err = Open(b.DataPath, 0644, nil)
		} else {
			db, err = b.OpenFunc(b.DataPath)
		}
		if err != nil {
			return DB{}, err
		}
		b.db, b.count = db, 0
	}
	b.count++
	return b.db, nil
}

// Free decreases the reference count. It close the DB if the count reaches
// zero.
func (b *RefCountBox) Free() {
	b.Lock()
	defer b.Unlock()

	b.count--
	if b.count == 0 {
		b.db.Close()
		b.db = DB{}
	}
}
