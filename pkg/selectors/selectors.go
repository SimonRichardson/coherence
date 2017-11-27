package selectors

import (
	"encoding/json"
	"sort"

	"github.com/spaolacci/murmur3"
)

// Key represents a Key in a cache
type Key string

// Equal checks to see if a Key matches another Key
func (k Key) Equal(b Key) bool {
	return k == b
}

// MarshalJSON represents a way of marshalling JSON to a slice of bytes
// Returns error on failure
func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(k))
}

// UnmarshalJSON represents a way of unmarshalling JSON from a series of bytes
// to a Field
// Returns error on failure
func (k *Key) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	(*k) = Key(s)
	return err
}

// Hash returns the hash (uint32) value of the Key union
func (k Key) Hash() uint32 {
	return murmur3.Sum32([]byte(k))
}

func (k Key) String() string {
	return string(k)
}

// Field represents a field value in a cache
type Field string

// Equal checks to see if a Field matches another Field
func (f Field) Equal(b Field) bool {
	return f == b
}

// MarshalJSON represents a way of marshalling JSON to a slice of bytes
// Returns error on failure
func (f Field) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(f))
}

// UnmarshalJSON represents a way of unmarshalling JSON from a series of bytes
// to a Field
// Returns error on failure
func (f *Field) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	(*f) = Field(s)
	return err
}

func (f Field) String() string {
	return string(f)
}

// KeyField defines the union of both the Key and Field
type KeyField struct {
	Key   Key   `json:"key"`
	Field Field `json:"field"`
}

// Hash returns the hash (uint32) value of the KeyField union
func (k KeyField) Hash() uint32 {
	return murmur3.Sum32([]byte(k.Key.String() + k.Field.String()))
}

// FieldScore represents the union of both a Field and a Score
type FieldScore struct {
	Field Field `json:"field"`
	Score int64 `json:"score"`
}

// ChangeSet defines success or failures when inserting into the storage. Each
// member is attached accordingly
type ChangeSet struct {
	Success []Field `json:"success"`
	Failure []Field `json:"failure"`
}

// Equal checks to see if a ChangeSet matches another ChangeSet
func (c ChangeSet) Equal(v ChangeSet) bool {
	return fieldsEqual(c.Success, v.Success) &&
		fieldsEqual(c.Failure, v.Failure)
}

// Append a new ChangeSet to the existing ChangeSet
func (c ChangeSet) Append(v ChangeSet) ChangeSet {
	return ChangeSet{
		Success: append(c.Success, v.Success...),
		Failure: append(c.Failure, v.Failure...),
	}
}

func fieldsEqual(a, b []Field) bool {
	if len(a) != len(b) {
		return false
	}

	// Make sure we're sorted, as the underlying data structure might not be
	// dependant on order
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})

	sort.Slice(b, func(i, j int) bool {
		return b[i] < b[j]
	})

	for k, v := range a {
		if !v.Equal(b[k]) {
			return false
		}
	}

	return true
}

// Presence represents what state a cache item is in the underlying storage.
type Presence struct {
	Present  bool
	Inserted bool
	Score    int64
}

// Equal checks to see if a Presence matches another Presences
func (p Presence) Equal(b Presence) bool {
	return p.Inserted == b.Inserted && p.Present == b.Present && p.Score == b.Score
}

// Clue represents if a value needs repairing and to what end.
type Clue struct {
	Ignore bool
	Insert bool
	Key    Key
	Field  Field
	Score  int64
	Quorum bool
}

// SetKeyField allows the setting of the key and field on the clue.
// This does not mutate the Clue
func (c Clue) SetKeyField(key Key, field Field) Clue {
	return Clue{
		Key:    key,
		Field:  field,
		Ignore: c.Ignore,
		Insert: c.Insert,
		Score:  c.Score,
		Quorum: c.Quorum,
	}
}
