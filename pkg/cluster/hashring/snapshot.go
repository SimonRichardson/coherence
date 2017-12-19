package hashring

import (
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
)

// Snapshot defines a way to snapshot a series of nodes at a specific time.
type Snapshot interface {

	// WriteSnapshot returns a set of nodes for a specific time. Nodes which are
	// used from the Read Snapshot are not guaranteed to succeed for longer than
	// their purpose.
	// It is not recommended to store the nodes locally as they may not be the same
	// nodes over time.
	// The function commits the values to the blooms once they've been written
	WriteSnapshot(selectors.Key, selectors.Quorum) ([]nodes.Node, func([]uint32) error)

	// ReadSnapshot returns a set of nodes for a specific time. Nodes which are
	// used from the Read Snapshot are not guaranteed to succeed for longer than
	// their purpose.
	// It is not recommended to store the nodes locally as they may not be the same
	// nodes over time.
	ReadSnapshot(selectors.Key, selectors.Quorum) []nodes.Node
}
