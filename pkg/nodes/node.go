package nodes

import (
	"github.com/trussle/coherence/pkg/selectors"
)

type Node interface {

	// Insert defines a way to insert some members into the store that's associated
	// with the key
	Insert(selectors.Key, []selectors.FieldScore) <-chan selectors.Element

	// Delete removes a set of members associated with a key with in the store
	Delete(selectors.Key, []selectors.FieldScore) <-chan selectors.Element

	// Keys returns all the keys with in the store
	Keys() <-chan selectors.Element

	// Size defines a way to find the size associated with the key
	Size(selectors.Key) <-chan selectors.Element

	// Members defines a way to return all member keys associated with the key
	Members(selectors.Key) <-chan selectors.Element

	// Repair attempts to repair the store depending on the elements
	Repair([]selectors.KeyField) <-chan selectors.Element
}
