package store

import "github.com/trussle/coherence/pkg/selectors"

// Store represents a in-memory Key/Value implementation
type Store interface {

	// Insert takes a key and value and stores with in the underlying store.
	// Returns true if it's over writting an existing value.
	Insert(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error)

	// Delete removes a value associated with the key.
	// Returns true if the value is found when deleting.
	Delete(selectors.Key, []selectors.FieldValueScore) (selectors.ChangeSet, error)

	// Select retrieves a field and score associated with the store.
	// Returns true if the value found
	Select(selectors.Key, selectors.Field) (selectors.FieldValueScore, error)

	// Keys returns all the potential keys that are stored with in the store.
	Keys() ([]selectors.Key, error)

	// Size returns the number of members for the key are stored in the store.
	Size(selectors.Key) (int64, error)

	// Members returns the members associated for a key
	Members(selectors.Key) ([]selectors.Field, error)

	// Score returns the specific score for the field with in the key.
	Score(selectors.Key, selectors.Field) (selectors.Presence, error)

	// Repair attempts to repair the store depending on the elements
	Repair([]selectors.KeyFieldValue) error
}
