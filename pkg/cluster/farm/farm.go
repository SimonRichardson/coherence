package farm

import "github.com/SimonRichardson/coherence/pkg/selectors"

// Farm represents a in-memory Key/Value implementation
type Farm interface {

	// Insert takes a key and value and farms with in the underlying farm.
	// Returns ChangeSet of success and failure
	Insert(selectors.Key, []selectors.FieldValueScore, selectors.Quorum) (selectors.ChangeSet, error)

	// Delete removes a value associated with the key.
	// Returns ChangeSet of success and failure
	Delete(selectors.Key, []selectors.FieldValueScore, selectors.Quorum) (selectors.ChangeSet, error)

	// Select retrieves a field and score associated with the farm.
	// Returns Field, Value and Score if the value found
	Select(selectors.Key, selectors.Field, selectors.Quorum) (selectors.FieldValueScore, error)

	// Keys returns all the potential keys that are stored with in the farm.
	Keys() ([]selectors.Key, error)

	// Size returns the number of members for the key are stored in the farm.
	Size(selectors.Key) (int64, error)

	// Members returns the members associated for a key
	Members(selectors.Key) ([]selectors.Field, error)

	// Score returns the specific score for the field with in the key.
	Score(selectors.Key, selectors.Field) (selectors.Presence, error)

	// Repair attempts to repair the store depending on the elements
	Repair([]selectors.KeyFieldValue) error
}
