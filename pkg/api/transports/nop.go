package transports

import "github.com/SimonRichardson/coherence/pkg/selectors"

type Nop struct{}

// Insert takes a key and value and stores with in the underlying store.
// Returns ChangeSet of success and failure
func (Nop) Insert(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{}, nil
}

// Delete removes a value associated with the key.
// Returns ChangeSet of success and failure
func (Nop) Delete(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{}, nil
}

// Select retrieves a field and score associated with the store.
// Returns Field, Value and Score if the value found
func (Nop) Select(selectors.Key, selectors.Field) (selectors.FieldValueScore, error) {
	return selectors.FieldValueScore{}, nil
}

// Keys returns all the potential keys that are stored with in the store.
func (Nop) Keys() ([]selectors.Key, error) {
	return nil, nil
}

// Size returns the number of members for the key are stored in the store.
func (Nop) Size(selectors.Key) (int64, error) {
	return 0, nil
}

// Members returns the members associated for a key
func (Nop) Members(selectors.Key) ([]selectors.Field, error) {
	return nil, nil
}

// Score returns the specific score for the field with in the key.
func (Nop) Score(selectors.Key, selectors.Field) (selectors.Presence, error) {
	return selectors.Presence{}, nil
}

// Hash returns the transport unique hash
func (Nop) Hash() uint32 {
	return 0
}

// Host returns the transport host
func (Nop) Host() string {
	return ""
}
