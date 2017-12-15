package selectors

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"sort"

	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"github.com/trussle/harness/generators"
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

// Generate allows Field to be used within quickcheck scenarios.
func (Key) Generate(r *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(Key(generateASCIIString(r, size)))
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

// Generate allows Field to be used within quickcheck scenarios.
func (Field) Generate(r *rand.Rand, size int) reflect.Value {
	s := generateASCIIString(r, size)
	return reflect.ValueOf(Field(s))
}

func generateString(r *rand.Rand, size int) string {
	v := make([]byte, size)
	if _, err := r.Read(v); err != nil {
		panic(err)
	}
	return string(v)
}

func generateASCIIString(r *rand.Rand, size int) string {
	return generators.GenerateString(r, size)
}

func (f Field) String() string {
	return string(f)
}

// ValueScore represents both a value and score for the store
type ValueScore struct {
	Value []byte
	Score int64
}

// Equal checks to see if a ValueScore matches another ValueScore
func (f ValueScore) Equal(b ValueScore) bool {
	return bytesEqual(f.Value, b.Value) && f.Score == b.Score
}

// Generate allows ValueScore to be used within quickcheck scenarios.
func (ValueScore) Generate(r *rand.Rand, size int) reflect.Value {
	v := make([]byte, size)
	if _, err := r.Read(v); err != nil {
		panic(err)
	}
	return reflect.ValueOf(ValueScore{
		Value: v,
		Score: 0,
	})
}

// FieldValueScore represents a field, value and score for the store
type FieldValueScore struct {
	Field Field
	Value []byte
	Score int64
}

// Equal checks to see if a FieldValueScore matches another FieldValueScore
func (f FieldValueScore) Equal(b FieldValueScore) bool {
	return f.Field.Equal(b.Field) && bytesEqual(f.Value, b.Value) && f.Score == b.Score
}

// FieldScore returns a FieldScore from a FieldValueScore
func (f FieldValueScore) FieldScore() FieldScore {
	return FieldScore{
		Field: f.Field,
		Score: f.Score,
	}
}

// ValueScore returns a ValueScore from a FieldValueScore
func (f FieldValueScore) ValueScore() ValueScore {
	return ValueScore{
		Value: f.Value,
		Score: f.Score,
	}
}

// Generate allows FieldValueScore to be used within quickcheck scenarios.
func (FieldValueScore) Generate(r *rand.Rand, size int) reflect.Value {
	v := make([]byte, size)
	if _, err := r.Read(v); err != nil {
		panic(err)
	}
	return reflect.ValueOf(FieldValueScore{
		Field: Field(generateASCIIString(r, size)),
		Value: v,
		Score: 0,
	})
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
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

// KeyFieldValue defines the union of both the Key, Field and Value
type KeyFieldValue struct {
	Key   Key    `json:"key"`
	Field Field  `json:"field"`
	Value []byte `json:"value"`
}

// Hash returns the hash (uint32) value of the KeyField union
func (k KeyFieldValue) Hash() uint32 {
	return murmur3.Sum32(append(
		[]byte(k.Key.String()+k.Field.String()),
		k.Value...,
	))
}

// FieldValue represents the union of both a Field and a Value
type FieldValue struct {
	Field Field  `json:"field"`
	Value []byte `json:"value"`
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
		Success: unique(c.Success, v.Success),
		Failure: unique(c.Failure, v.Failure),
	}
}

func unique(a, b []Field) []Field {
	x := make(map[Field]struct{})
	for _, v := range a {
		x[v] = struct{}{}
	}

	for _, v := range b {
		x[v] = struct{}{}
	}

	var (
		index int
		res   = make([]Field, len(x))
	)
	for k := range x {
		res[index] = k
		index++
	}

	return res
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
	Value  []byte
	Score  int64
	Quorum bool
}

// SetKeyFieldValue allows the setting of the key and field on the clue.
// This does not mutate the Clue
func (c Clue) SetKeyFieldValue(key Key, field Field, value []byte) Clue {
	return Clue{
		Key:    key,
		Field:  field,
		Value:  value,
		Ignore: c.Ignore,
		Insert: c.Insert,
		Score:  c.Score,
		Quorum: c.Quorum,
	}
}

// Quorum defines the types of different consensus algorithms we want to achieve
// These are various strategy patterns.
type Quorum string

const (
	// One defines the need for only one node to be satisfied
	One Quorum = "one"

	// Strong defines the need for all nodes to be read and // written against,
	// anything less is a failure
	Strong Quorum = "strong"

	// Consensus defines the need for only 51% or more nodes to be read and
	// written against, anything less is a failure
	Consensus Quorum = "consensus"
)

func (q Quorum) String() string {
	return string(q)
}

// ParseQuorum returns a valid Quorum otherwise returns an error
func ParseQuorum(s string) (Quorum, error) {
	switch s {
	case One.String(), Strong.String(), Consensus.String():
		return Quorum(s), nil
	default:
		return Quorum(""), errors.Errorf("unknown quorum %q", s)
	}
}
