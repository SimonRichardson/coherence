package store

import "github.com/SimonRichardson/coherence/pkg/selectors"

// StoreContext holds the base of the store
type StoreContext interface {
	String() string
}

// Store represents a in-memory Key/Value implementation
type Store interface {
	StoreContext

	// Insert takes a key and value and stores with in the underlying store.
	// Returns ChangeSet of success and failure
	Insert(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error)

	// Delete removes a value associated with the key.
	// Returns ChangeSet of success and failure
	Delete(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error)

	// Select retrieves a field and score associated with the store.
	// Returns Field, Value and Score if the value found
	Select(selectors.Key, selectors.Field) (selectors.FieldValueScore, error)

	// Keys returns all the potential keys that are stored with in the store.
	Keys() ([]selectors.Key, error)

	// Size returns the number of members for the key are stored in the store.
	Size(selectors.Key) (int64, error)

	// Members returns the members associated for a key
	Members(selectors.Key) ([]selectors.Field, error)

	// Score returns the specific score for the field with in the key.
	Score(selectors.Key, selectors.Field) (selectors.Presence, error)
}
