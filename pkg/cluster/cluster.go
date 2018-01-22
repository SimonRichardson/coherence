package cluster

import "github.com/SimonRichardson/coherence/pkg/cluster/members"

// Peer represents the node with in the cluster.
type Peer interface {
	members.EventBus

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

	// Close and shutdown the peer
	Close()
}
