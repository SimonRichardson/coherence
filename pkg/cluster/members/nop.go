package members

type nopMembers struct {
}

// NewNopMembers creates a new members list to join.
func NewNopMembers() Members { return nopMembers{} }

func (nopMembers) Join() (int, error)              { return 0, nil }
func (nopMembers) Leave() error                    { return nil }
func (nopMembers) MemberList() MemberList          { return nopMemberList{} }
func (nopMembers) Walk(func(PeerInfo) error) error { return nil }
func (nopMembers) Close() error                    { return nil }

func (nopMembers) RegisterEventHandler(EventHandler) error   { return nil }
func (nopMembers) DeregisterEventHandler(EventHandler) error { return nil }
func (nopMembers) DispatchEvent(Event) error                 { return nil }

type nopMemberList struct{}

func (nopMemberList) NumMembers() int   { return 0 }
func (nopMemberList) LocalNode() Member { return nopMember{} }
func (nopMemberList) Members() []Member { return make([]Member, 0) }

type nopMember struct{}

func (nopMember) Name() string    { return "" }
func (nopMember) Address() string { return "0.0.0.0:0" }
