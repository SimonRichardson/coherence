package selectors

// ElementType defines the type of element to expect over the wire.
type ElementType int

const (
	// ErrorElementType describes an element with a error payload
	ErrorElementType ElementType = iota

	// Int64ElementType describes an element with an int64 payload
	Int64ElementType

	// KeysElementType describes an element with an a slice of keys payload
	KeysElementType

	// FieldsElementType describes an element with a slice of fields payload
	FieldsElementType

	// ChangeSetElementType describes an element with an change set payload
	ChangeSetElementType

	// PresenceElementType describes an element with an presence payload
	PresenceElementType

	// FieldScoreElementType describes an element with a field score payload
	FieldScoreElementType

	// FieldValueScoreElementType describes an element with a field, value score payload
	FieldValueScoreElementType
)

// Element combines a submitted key with the resulting values. If there was an
// error while selecting a key, the error field will be populated.
type Element interface {
	Type() ElementType
}

// ErrorElement defines a struct that is a container for errors.
type ErrorElement struct {
	typ ElementType
	err error
}

// NewErrorElement creates a new ErrorElement
func NewErrorElement(err error) *ErrorElement {
	return &ErrorElement{ErrorElementType, err}
}

// Type defines the type associated with the ErrorElement
func (e *ErrorElement) Type() ElementType { return e.typ }

// Error defines the error associated with the ErrorElement
func (e *ErrorElement) Error() error { return e.err }

type errorElement interface {
	Error() error
}

// ErrorFromElement attempts to get an error from the element if it exists.
func ErrorFromElement(e Element) error {
	if v, ok := e.(errorElement); ok {
		return v.Error()
	}
	return nil
}

// Int64Element defines a struct that is a container for errors.
type Int64Element struct {
	typ ElementType
	val int64
}

// NewInt64Element creates a new Int64Element
func NewInt64Element(val int64) *Int64Element {
	return &Int64Element{Int64ElementType, val}
}

// Type defines the type associated with the Int64Element
func (e *Int64Element) Type() ElementType { return e.typ }

// Int64 defines the int64 associated with the 6464Element
func (e *Int64Element) Int64() int64 { return e.val }

type int64Element interface {
	Int64() int64
}

// Int64FromElement attempts to get an int from the element if it exists.
func Int64FromElement(e Element) int64 {
	if v, ok := e.(int64Element); ok {
		return v.Int64()
	}
	return -1
}

// KeysElement defines a struct that is a container for errors.
type KeysElement struct {
	typ ElementType
	val []Key
}

// NewKeysElement creates a new KeysElement
func NewKeysElement(val []Key) *KeysElement {
	return &KeysElement{KeysElementType, val}
}

// Type defines the type associated with the KeysElement
func (e *KeysElement) Type() ElementType { return e.typ }

// Keys defines the []Key associated with the KeysElement
func (e *KeysElement) Keys() []Key { return e.val }

type keysElement interface {
	Keys() []Key
}

// KeysFromElement attempts to get an int from the element if it exists.
func KeysFromElement(e Element) []Key {
	if v, ok := e.(keysElement); ok {
		return v.Keys()
	}
	return make([]Key, 0)
}

// FieldsElement defines a struct that is a container for errors.
type FieldsElement struct {
	typ ElementType
	val []Field
}

// NewFieldsElement creates a new FieldsElement
func NewFieldsElement(val []Field) *FieldsElement {
	return &FieldsElement{FieldsElementType, val}
}

// Type defines the type associated with the FieldsElement
func (e *FieldsElement) Type() ElementType { return e.typ }

// Fields defines the []Field associated with the FieldsElement
func (e *FieldsElement) Fields() []Field { return e.val }

type fieldsElement interface {
	Fields() []Field
}

// FieldsFromElement attempts to get an int from the element if it exists.
func FieldsFromElement(e Element) []Field {
	if v, ok := e.(fieldsElement); ok {
		return v.Fields()
	}
	return make([]Field, 0)
}

// ChangeSetElement defines a struct that is a container for errors.
type ChangeSetElement struct {
	typ ElementType
	val ChangeSet
}

// NewChangeSetElement creates a new ChangeSetElement
func NewChangeSetElement(val ChangeSet) *ChangeSetElement {
	return &ChangeSetElement{ChangeSetElementType, val}
}

// Type defines the type associated with the ChangeSetElement
func (e *ChangeSetElement) Type() ElementType { return e.typ }

// ChangeSet defines the changeSet associated with the ChangeSetElement
func (e *ChangeSetElement) ChangeSet() ChangeSet { return e.val }

type changeSetElement interface {
	ChangeSet() ChangeSet
}

// ChangeSetFromElement attempts to get an changeSet from the element if it exists.
func ChangeSetFromElement(e Element) ChangeSet {
	if v, ok := e.(changeSetElement); ok {
		return v.ChangeSet()
	}
	return ChangeSet{}
}

// PresenceElement defines a struct that is a container for errors.
type PresenceElement struct {
	typ ElementType
	val Presence
}

// NewPresenceElement creates a new PresenceElement
func NewPresenceElement(val Presence) *PresenceElement {
	return &PresenceElement{PresenceElementType, val}
}

// Type defines the type associated with the PresenceElement
func (e *PresenceElement) Type() ElementType { return e.typ }

// Presence defines the presence associated with the PresenceElement
func (e *PresenceElement) Presence() Presence { return e.val }

type presenceElement interface {
	Presence() Presence
}

// PresenceFromElement attempts to get an presence from the element if it exists.
func PresenceFromElement(e Element) Presence {
	if v, ok := e.(presenceElement); ok {
		return v.Presence()
	}
	return Presence{}
}

// FieldScoreElement defines a struct that is a container for errors.
type FieldScoreElement struct {
	typ ElementType
	val FieldScore
}

// NewFieldScoreElement creates a new FieldScoreElement
func NewFieldScoreElement(val FieldScore) *FieldScoreElement {
	return &FieldScoreElement{FieldScoreElementType, val}
}

// Type defines the type associated with the FieldScoreElement
func (e *FieldScoreElement) Type() ElementType { return e.typ }

// FieldScore defines the fieldScore associated with the FieldScoreElement
func (e *FieldScoreElement) FieldScore() FieldScore { return e.val }

type fieldScoreElement interface {
	FieldScore() FieldScore
}

// FieldScoreFromElement attempts to get an fieldScore from the element if it exists.
func FieldScoreFromElement(e Element) FieldScore {
	if v, ok := e.(fieldScoreElement); ok {
		return v.FieldScore()
	}
	return FieldScore{}
}

// FieldValueScoreElement defines a struct that is a container for errors.
type FieldValueScoreElement struct {
	typ ElementType
	val FieldValueScore
}

// NewFieldValueScoreElement creates a new FieldValueScoreElement
func NewFieldValueScoreElement(val FieldValueScore) *FieldValueScoreElement {
	return &FieldValueScoreElement{FieldValueScoreElementType, val}
}

// Type defines the type associated with the FieldValueScoreElement
func (e *FieldValueScoreElement) Type() ElementType { return e.typ }

// FieldValueScore defines the fieldValueScore associated with the FieldValueScoreElement
func (e *FieldValueScoreElement) FieldValueScore() FieldValueScore { return e.val }

type fieldValueScoreElement interface {
	FieldValueScore() FieldValueScore
}

// FieldValueScoreFromElement attempts to get an fieldValueScore from the element if it exists.
func FieldValueScoreFromElement(e Element) FieldValueScore {
	if v, ok := e.(fieldValueScoreElement); ok {
		return v.FieldValueScore()
	}
	return FieldValueScore{}
}
