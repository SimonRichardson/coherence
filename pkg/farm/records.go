package farm

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/selectors"
)

type changeSetRecords struct {
	changeSet selectors.ChangeSet
	set       bool
	err       error
}

func (r *changeSetRecords) Add(v selectors.ChangeSet) {
	if r.set && !r.changeSet.Equal(v) {
		r.err = errors.New("variance detected from replication")
		return
	}
	r.changeSet = v
	r.set = true
}

func (r *changeSetRecords) Err() error {
	return r.err
}

func (r *changeSetRecords) ChangeSet() selectors.ChangeSet {
	return r.changeSet
}

type keysRecords struct {
	keys []selectors.Key
	set  bool
	err  error
}

func (r *keysRecords) Add(v []selectors.Key) {
	match := func(a, b []selectors.Key) bool {
		if len(a) != len(b) {
			return false
		}

		sort.Slice(a, func(i, j int) bool {
			return a[i].String() < a[j].String()
		})

		sort.Slice(b, func(i, j int) bool {
			return b[i].String() < b[j].String()
		})

		for k, v := range a {
			if v != b[k] {
				return false
			}
		}
		return true
	}
	if r.set && !match(r.keys, v) {
		r.err = errors.New("variance detected from replication")
		return
	}
	r.keys = v
	r.set = true
}

func (r *keysRecords) Err() error {
	return r.err
}

func (r *keysRecords) Keys() []selectors.Key {
	return r.keys
}

type fieldsRecords struct {
	fields []selectors.Field
	set    bool
	err    error
}

func (r *fieldsRecords) Add(v []selectors.Field) {
	match := func(a, b []selectors.Field) bool {
		if len(a) != len(b) {
			return false
		}

		sort.Slice(a, func(i, j int) bool {
			return a[i].String() < a[j].String()
		})

		sort.Slice(b, func(i, j int) bool {
			return b[i].String() < b[j].String()
		})

		for k, v := range a {
			if v != b[k] {
				return false
			}
		}
		return true
	}
	if r.set && !match(r.fields, v) {
		r.err = errors.New("variance detected from replication")
		return
	}
	r.fields = v
	r.set = true
}

func (r *fieldsRecords) Err() error {
	return r.err
}

func (r *fieldsRecords) Fields() []selectors.Field {
	return r.fields
}

type intRecords struct {
	integer int
	set     bool
	err     error
}

func (r *intRecords) Add(v int) {
	if r.set && r.integer != v {
		r.err = errors.New("variance detected from replication")
		return
	}
	r.integer = v
	r.set = true
}

func (r *intRecords) Err() error {
	return r.err
}

func (r *intRecords) Int() int {
	return r.integer
}
