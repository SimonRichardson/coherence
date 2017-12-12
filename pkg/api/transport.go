package api

import "github.com/trussle/coherence/pkg/selectors"

// TransportStrategy defines a way to create a transport
type TransportStrategy interface {

	// Apply applies a host to create a new api.Transport
	Apply(string) Transport
}

// Transport wraps the API transportation request to provide an agnostic
// approach to the store.
// As long as the API implements the following transportation service then
// any protocol can be used; http, gRPC, raw udp
type Transport interface {

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

	// Hash returns the transport unique hash
	Hash() uint32
}
