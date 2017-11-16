package cluster

import (
	"net"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/members"
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
	members  members.Members
	stop     chan chan struct{}
	callback func(Reason)
	logger   log.Logger
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
		stop:    make(chan chan struct{}),
		callback: func(Reason) {
			level.Warn(logger).Log("reason", "alone")
		},
		logger: logger,
	}
}

func (p *peer) run() {
	ticker := time.NewTicker(defaultMembersBroadcastInterval)
	defer ticker.Stop()

	members := p.members.MemberList()
	for {
		select {
		case <-ticker.C:
			// Notify the callback if below a threshold.
			if num := members.NumMembers(); num <= defaultLowMembersThreshold {
				p.callback(ReasonAlone)
			}

		case c := <-p.stop:
			close(c)
			return
		}
	}
}

// Close out the API
func (p *peer) Close() {
	c := make(chan struct{})
	p.stop <- c
	<-c
}

func (p *peer) Join() (int, error) {
	numNodes, err := p.members.Join()
	if err != nil {
		return 0, err
	}

	go p.run()

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
func (p *peer) Current(peerType members.PeerType) (res []string, err error) {
	err = p.members.Walk(func(info members.PeerInfo) error {
		if peerType == PeerTypeStore && info.Type == PeerTypeStore {
			res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
		}
		return nil
	})
	return
}

func (p *peer) Listen(fn func(Reason)) error {
	p.callback = fn
	return nil
}

func memberNames(m []members.Member) []string {
	res := make([]string, len(m))
	for k, v := range m {
		res[k] = v.Name()
	}
	return res
}
