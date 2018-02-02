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

// MemberEventType is the potential event type for member event
type MemberEventType int

const (
	// EventMemberJoined notified from a cluster when a member has joined
	EventMemberJoined MemberEventType = iota

	// EventMemberLeft notified from a cluster when a member has left
	EventMemberLeft

	// EventMemberFailed notified from a cluster when a member has failed
	EventMemberFailed

	// EventMemberUpdated notified from a cluster when a member has updated
	EventMemberUpdated
)

// MemberEvent is an event that represents when a member has changed in the
// cluster
type MemberEvent struct {
	EventType MemberEventType
	Members   []Member
}

// NewMemberEvent creates a new MemberEvent with the correct dependencies
func NewMemberEvent(eventType MemberEventType, members []Member) Event {
	return &MemberEvent{
		EventType: eventType,
		Members:   members,
	}
}

// Type returns the EventType of the Event
func (MemberEvent) Type() EventType {
	return EventMember
}

// UserEvent is an event that represents when a user sends a message to the
// cluster
type UserEvent struct {
	Name    string
	Payload []byte
}

// NewUserEvent creates a new UserEvent with the correct dependencies
func NewUserEvent(name string, payload []byte) Event {
	return &UserEvent{
		Name:    name,
		Payload: payload,
	}
}

// Type returns the EventType of the Event
func (UserEvent) Type() EventType {
	return EventUser
}

// QueryEvent is an event that represents when a query from the cluster that
// needs to be answered.
type QueryEvent struct {
	Name    string
	Payload []byte
	query   *serf.Query
}

// NewQueryEvent creates a new QueryEvent with the correct dependencies
func NewQueryEvent(name string, payload []byte, query *serf.Query) Event {
	return &QueryEvent{
		Name:    name,
		Payload: payload,
		query:   query,
	}
}

// Type returns the EventType of the Event
func (QueryEvent) Type() EventType {
	return EventQuery
}

// ErrorEvent is an event that represents when an error comes from the cluster
type ErrorEvent struct {
	Error error
}

// NewErrorEvent creates a new ErrorEvent with the correct dependencies
func NewErrorEvent(err error) Event {
	return &ErrorEvent{
		Error: err,
	}
}

// Type returns the EventType of the Event
func (ErrorEvent) Type() EventType {
	return EventError
}
