package members

type nopMembers struct {
}

// NewNopMembers creates a new members list to join.
func NewNopMembers() Members { return nopMembers{} }

func (r nopMembers) Join() (int, error)              { return 0, nil }
func (r nopMembers) Leave() error                    { return nil }
func (r nopMembers) MemberList() MemberList          { return nopMemberList{} }
func (r nopMembers) Walk(func(PeerInfo) error) error { return nil }
func (r nopMembers) Close() error                    { return nil }

type nopMemberList struct{}

func (r nopMemberList) NumMembers() int   { return 0 }
func (r nopMemberList) LocalNode() Member { return nopMember{} }
func (r nopMemberList) Members() []Member { return make([]Member, 0) }

type nopMember struct{}

func (r nopMember) Name() string { return "" }
