package members

import "github.com/hashicorp/serf/serf"

// EventType is the potential event type for member event
type EventType int

const (
	// EventMember was received from the cluster
	EventMember EventType = iota

	// EventQuery was received from the cluster
	EventQuery

	// EventUser was received from the cluster
	EventUser

	// EventError was received from the cluster
	EventError
)

// Event is the member event to be acted upon
type Event interface {
	// Type is one of the EventType
	Type() EventType
}

type MemberEventType int

const (
	EventMemberJoined MemberEventType = iota
	EventMemberLeft
	EventMemberFailed
	EventMemberUpdated
)

type MembersEvent struct {
	EventType MemberEventType
	Members   []Member
}

func NewMembersEvent(eventType MemberEventType, members []Member) Event {
	return &MembersEvent{
		EventType: eventType,
		Members:   members,
	}
}

func (MembersEvent) Type() EventType {
	return EventMember
}

type UserEvent struct {
	Name    string
	Payload []byte
}

func NewUserEvent(name string, payload []byte) Event {
	return &UserEvent{
		Name:    name,
		Payload: payload,
	}
}

func (UserEvent) Type() EventType {
	return EventUser
}

type QueryEvent struct {
	Name    string
	Payload []byte
	query   *serf.Query
}

func NewQueryEvent(name string, payload []byte, query *serf.Query) Event {
	return &QueryEvent{
		Name:    name,
		Payload: payload,
		query:   query,
	}
}

func (QueryEvent) Type() EventType {
	return EventQuery
}

type ErrorEvent struct {
	Error error
}

func NewErrorEvent(err error) Event {
	return &ErrorEvent{
		Error: err,
	}
}

func (ErrorEvent) Type() EventType {
	return EventError
}
