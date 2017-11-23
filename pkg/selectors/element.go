package selectors

// ElementType defines the type of element to expect over the wire.
type ElementType int

const (
	// ErrorElementType describes an element with a error payload
	ErrorElementType ElementType = iota

	// IntElementType describes an element with an amount payload
	IntElementType

	// KeysElementType describes an element with an amount payload
	KeysElementType

	// FieldsElementType describes an element with an amount payload
	FieldsElementType

	// ChangeSetElementType describes an element with an amount payload
	ChangeSetElementType
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

// IntElement defines a struct that is a container for errors.
type IntElement struct {
	typ ElementType
	val int
}

// NewIntElement creates a new IntElement
func NewIntElement(val int) *IntElement {
	return &IntElement{IntElementType, val}
}

// Type defines the type associated with the IntElement
func (e *IntElement) Type() ElementType { return e.typ }

// Int defines the int associated with the IntElement
func (e *IntElement) Int() int { return e.val }

type intElement interface {
	Int() int
}

// IntFromElement attempts to get an int from the element if it exists.
func IntFromElement(e Element) int {
	if v, ok := e.(intElement); ok {
		return v.Int()
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
