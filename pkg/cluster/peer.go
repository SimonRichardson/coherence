package cluster

import (
	"net"
	"strconv"
	"time"

	"github.com/SimonRichardson/coherence/pkg/cluster/members"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

const (
	defaultBroadcastTimeout         = time.Second * 10
	defaultMembersBroadcastInterval = time.Second * 5
	defaultLowMembersThreshold      = 1
)

const (
	// PeerTypeStore serves the store API
	PeerTypeStore members.PeerType = "store"
)

// ParsePeerType parses a potential peer type and errors out if it's not a known
// valid type.
func ParsePeerType(t string) (members.PeerType, error) {
	switch t {
	case "store":
		return members.PeerType(t), nil
	default:
		return "", errors.Errorf("invalid peer type (%s)", t)
	}
}

// peer represents the node with in the cluster.
type peer struct {
	members members.Members
	logger  log.Logger
}

// NewPeer creates or joins a cluster with the existing peers.
// We will listen for cluster communications on the bind addr:port.
// We advertise a PeerType HTTP API, reachable on apiPort.
func NewPeer(
	members members.Members,
	logger log.Logger,
) Peer {
	return &peer{
		members: members,
		logger:  logger,
	}
}

// Close out the API
func (p *peer) Close() {}

func (p *peer) Join() (int, error) {
	numNodes, err := p.members.Join()
	if err != nil {
		return 0, err
	}

	return numNodes, nil
}

// Leave the cluster.
func (p *peer) Leave() error {
	// Ignore this timeout for now, serf uses a config timeout.
	return p.members.Leave()
}

// Name returns unique ID of this peer in the cluster.
func (p *peer) Name() string {
	return p.members.MemberList().LocalNode().Name()
}

// Address returns host:port of this peer in the cluster.
func (p *peer) Address() string {
	return p.members.MemberList().LocalNode().Address()
}

// ClusterSize returns the total size of the cluster from this node's
// perspective.
func (p *peer) ClusterSize() int {
	return p.members.MemberList().NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *peer) State() map[string]interface{} {
	members := p.members.MemberList()
	return map[string]interface{}{
		"self":        members.LocalNode().Name(),
		"members":     memberNames(members.Members()),
		"num_members": members.NumMembers(),
	}
}

// Current API host:ports for the given type of node.
// IncludeLocal doesn't add the local cluster node to the resulting set.
func (p *peer) Current(peerType members.PeerType, includeLocal bool) (res []string, err error) {
	localName := p.Name()
	err = p.members.Walk(func(info members.PeerInfo) error {
		if !includeLocal && info.Name == localName {
			return nil
		}

		if peerType == PeerTypeStore && info.Type == PeerTypeStore {
			res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
		}
		return nil
	})
	return
}

func (p *peer) RegisterEventHandler(fn members.EventHandler) error {
	return p.members.RegisterEventHandler(fn)
}

func (p *peer) DeregisterEventHandler(fn members.EventHandler) error {
	return p.members.DeregisterEventHandler(fn)
}

func (p *peer) DispatchEvent(e members.Event) error {
	return p.members.DispatchEvent(e)
}

func memberNames(m []members.Member) []string {
	res := make([]string, len(m))
	for k, v := range m {
		res[k] = v.Name()
	}
	return res
}
