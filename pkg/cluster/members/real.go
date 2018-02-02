package members

import (
	"fmt"
	"io"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/cmd/serf/command/agent"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

const (
	defaultAgentLogLevel = "WARN"
)

type realMembers struct {
	config        Config
	mutex         sync.Mutex
	agent         *agent.Agent
	members       *serf.Serf
	eventHandlers map[EventHandler]agent.EventHandler
	logger        log.Logger
}

// NewRealMembers creates a new members list to join.
func NewRealMembers(config Config, logger log.Logger) (Members, error) {
	actor, err := agent.Create(transformConfig(config))
	if err != nil {
		return nil, err
	}

	if err := actor.Start(); err != nil {
		return nil, err
	}

	return &realMembers{
		config:        config,
		mutex:         sync.Mutex{},
		agent:         actor,
		members:       actor.Serf(),
		eventHandlers: make(map[EventHandler]agent.EventHandler),
		logger:        logger,
	}, nil
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

func (r *realMembers) RegisterEventHandler(fn EventHandler) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	eh := realEventHandler{
		fn:     fn,
		logger: log.With(r.logger, "component", "event_handler"),
	}

	r.eventHandlers[fn] = eh
	r.agent.RegisterEventHandler(eh)
	return nil
}

func (r *realMembers) DeregisterEventHandler(fn EventHandler) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if eh, ok := r.eventHandlers[fn]; ok {
		delete(r.eventHandlers, fn)
		r.agent.DeregisterEventHandler(eh)
	}

	return nil
}

func (r *realMembers) DispatchEvent(e Event) error {
	switch t := e.(type) {
	case *UserEvent:
		return r.agent.UserEvent(t.Name, t.Payload, true)
	default:
		return errors.Errorf("Unsupported event type %v", e.Type())
	}
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

func (r *realMember) Address() string {
	return r.member.Address()
}

type realEventHandler struct {
	fn     EventHandler
	logger log.Logger
}

func (h realEventHandler) HandleEvent(event serf.Event) {
	switch t := event.(type) {
	case serf.MemberEvent:
		h.handleMemberEvent(t)
	case serf.UserEvent:
		h.processEvent(NewUserEvent(t.Name, t.Payload))
	case *serf.Query:
		h.processEvent(NewQueryEvent(t.Name, t.Payload, t))
	case error:
		h.processEvent(NewErrorEvent(t))
	default:
		level.Warn(h.logger).Log("reason", "unhandled event", "event_type", event.EventType())
	}
}

func (h realEventHandler) handleMemberEvent(event serf.MemberEvent) {
	var t MemberEventType

	switch event.Type {
	case serf.EventMemberJoin:
		t = EventMemberJoined
	case serf.EventMemberFailed:
		t = EventMemberFailed
	case serf.EventMemberLeave:
		t = EventMemberLeft
	case serf.EventMemberUpdate:
		t = EventMemberUpdated
	default:
		// We don't know how to handle this, so bubble it up to the receiver.
		err := errors.Errorf("unexpected member event %q", event.Type.String())
		h.processEvent(NewErrorEvent(err))
		return
	}

	var m []Member
	for _, v := range event.Members {
		m = append(m, eventMember{
			name: v.Name,
			addr: v.Addr.String(),
			port: int(v.Port),
		})
	}

	h.processEvent(NewMemberEvent(t, m))
}

func (h realEventHandler) processEvent(event Event) {
	if event == nil {
		return
	}

	if err := h.fn.HandleEvent(event); err != nil {
		level.Warn(h.logger).Log("err", err)
	}
}

func transformConfig(config Config) (*agent.Config, *serf.Config, io.Writer) {
	agentConfig := agent.DefaultConfig()
	agentConfig.LogLevel = defaultAgentLogLevel
	if config.clientAddr != "" {
		agentConfig.RPCAddr = fmt.Sprintf("%s:%d", config.clientAddr, config.clientPort)
	}

	serfConfig := serf.DefaultConfig()

	serfConfig.NodeName = config.nodeName
	serfConfig.MemberlistConfig.BindAddr = config.bindAddr
	serfConfig.MemberlistConfig.BindPort = config.bindPort
	if config.advertiseAddr != "" {
		serfConfig.MemberlistConfig.AdvertiseAddr = config.advertiseAddr
		serfConfig.MemberlistConfig.AdvertisePort = config.advertisePort
	}
	serfConfig.MemberlistConfig.LogOutput = config.logOutput
	serfConfig.LogOutput = config.logOutput
	serfConfig.BroadcastTimeout = config.broadcastTimeout
	serfConfig.Tags = encodePeerInfoTag(PeerInfo{
		Name:    config.nodeName,
		Type:    config.peerType,
		APIAddr: config.apiAddr,
		APIPort: config.apiPort,
	})
	serfConfig.Init()

	return agentConfig, serfConfig, config.logOutput
}

type eventMember struct {
	name string
	addr string
	port int
}

func (e eventMember) Name() string {
	return e.name
}

func (e eventMember) Address() string {
	return fmt.Sprintf("%s:%d", e.addr, e.port)
}
