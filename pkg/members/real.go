package members

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

type realMembers struct {
	config  Config
	members *serf.Serf
	logger  log.Logger
}

// NewRealMembers creates a new members list to join.
func NewRealMembers(config Config, logger log.Logger) (Members, error) {
	members, err := serf.Create(transformConfig(config))
	if err != nil {
		return nil, err
	}

	return &realMembers{config, members, logger}, nil
}

func (r *realMembers) Join() (int, error) {
	return r.members.Join(r.config.existing, true)
}

func (r *realMembers) Leave() error {
	return r.members.Leave()
}

func (r *realMembers) MemberList() MemberList {
	return &realMemberList{
		r.members.Memberlist(),
		r.logger,
	}
}

func (r *realMembers) Walk(fn func(PeerInfo) error) error {
	for _, v := range r.members.Members() {
		if v.Status != serf.StatusAlive {
			continue
		}

		if info, err := decodePeerInfoTag(v.Tags); err == nil {
			if e := fn(info); e != nil {
				return err
			}
		}
	}
	return nil
}

func (r *realMembers) Close() error {
	if err := r.members.Leave(); err != nil {
		level.Warn(r.logger).Log("err", err)
	}
	return r.members.Shutdown()
}

type realMemberList struct {
	list   *memberlist.Memberlist
	logger log.Logger
}

func (r *realMemberList) NumMembers() int {
	return r.list.NumMembers()
}

func (r *realMemberList) LocalNode() Member {
	return &realMember{r.list.LocalNode()}
}

func (r *realMemberList) Members() []Member {
	m := r.list.Members()
	n := make([]Member, len(m))
	for k, v := range m {
		n[k] = &realMember{v}
	}
	return n
}

type realMember struct {
	member *memberlist.Node
}

func (r *realMember) Name() string {
	return r.member.Name
}

func transformConfig(config Config) *serf.Config {
	c := serf.DefaultConfig()

	c.NodeName = config.nodeName
	c.MemberlistConfig.BindAddr = config.bindAddr
	c.MemberlistConfig.BindPort = config.bindPort
	if config.advertiseAddr != "" {
		c.MemberlistConfig.AdvertiseAddr = config.advertiseAddr
		c.MemberlistConfig.AdvertisePort = config.advertisePort
	}
	c.LogOutput = config.logOutput
	c.BroadcastTimeout = config.broadcastTimeout
	c.Tags = encodePeerInfoTag(PeerInfo{
		Name:    config.nodeName,
		Type:    config.peerType,
		APIAddr: config.bindAddr,
		APIPort: config.bindPort,
	})

	return c
}
