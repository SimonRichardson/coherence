package nodes

import (
	"github.com/trussle/coherence/pkg/selectors"
)

// Node describes a type that can communicate with various node implementations
// in a generic concurrent manor.
type Node interface {

	// Insert defines a way to insert some members into the store that's associated
	// with the key
	Insert(selectors.Key, []selectors.FieldScore) <-chan selectors.Element

	// Delete removes a set of members associated with a key with in the store
	Delete(selectors.Key, []selectors.FieldScore) <-chan selectors.Element

	// Select retrieves a single element from the store
	Select(selectors.Key, selectors.Field) <-chan selectors.Element

	// Keys returns all the keys with in the store
	Keys() <-chan selectors.Element

	// Size defines a way to find the size associated with the key
	Size(selectors.Key) <-chan selectors.Element

	// Members defines a way to return all member keys associated with the key
	Members(selectors.Key) <-chan selectors.Element

	// Score returns the value of the field in a key
	Score(selectors.Key, selectors.Field) <-chan selectors.Element
}
