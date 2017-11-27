package farm

import "github.com/trussle/coherence/pkg/selectors"

// Farm holds identifiers with associated records
type Farm interface {

	// Insert defines a way to insert some members into the store that's associated
	// with the key
	Insert(selectors.Key, []selectors.FieldScore) (selectors.ChangeSet, error)

	// Delete removes a set of members associated with a key with in the store
	Delete(selectors.Key, []selectors.FieldScore) (selectors.ChangeSet, error)

	// Keys returns all the keys with in the store
	Keys() ([]selectors.Key, error)

	// Size defines a way to find the size associated with the key
	Size(selectors.Key) (int64, error)

	// Members defines a way to return all member keys associated with the key
	Members(selectors.Key) ([]selectors.Field, error)

	// Score defines a way to find out the score associated with a field with in a
	// key
	Score(selectors.Key, selectors.Field) (selectors.Presence, error)

	// Repair attempts to repair the store depending on the elements
	Repair([]selectors.KeyField) error
}
