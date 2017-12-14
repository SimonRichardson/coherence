package cluster

import "github.com/SimonRichardson/coherence/pkg/cluster/members"

// Reason defines a type of reason a peer will notify the callback
type Reason string

const (
	// ReasonAlone represents a peer that is alone and an action is required.
	ReasonAlone Reason = "alone"

	// ReasonAccompanied represents a peer that is not alone, but accompanied.
	ReasonAccompanied Reason = "accompanied"
)

// Peer represents the node with in the cluster.
type Peer interface {

	// Join the cluster
	Join() (int, error)

	// Leave the cluster.
	Leave() error

	// Name returns unique ID of this peer in the cluster.
	Name() string

	// Address returns host:port of this peer in the cluster.
	Address() string

	// ClusterSize returns the total size of the cluster from this node's
	// perspective.
	ClusterSize() int

	// State returns a JSON-serializable dump of cluster state.
	// Useful for debug.
	State() map[string]interface{}

	// Current API host:ports for the given type of node.
	// Bool defines if you want to include the current local node.
	Current(peerType members.PeerType, includeLocal bool) ([]string, error)

	// Listen registers a callback for potential issues with the peer. For example
	// if the peer is on it's own.
	Listen(func(Reason)) error

	// Close and shutdown the peer
	Close()
}
