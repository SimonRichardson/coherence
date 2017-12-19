package nodes

import "github.com/SimonRichardson/coherence/pkg/selectors"
import "github.com/spaolacci/murmur3"

const (
	defaultHash = 0
)

type nop struct{}

// NewNop creates a nop Node
func NewNop() Node {
	return nop{}
}

func (nop) Insert(key selectors.Key, members []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewChangeSetElement(defaultHash, selectors.ChangeSet{
			Success: make([]selectors.Field, 0),
			Failure: extractFields(members),
		})
	}()
	return ch
}

func (nop) Delete(key selectors.Key, members []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewChangeSetElement(defaultHash, selectors.ChangeSet{
			Success: make([]selectors.Field, 0),
			Failure: extractFields(members),
		})
	}()
	return ch
}

func (nop) Select(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewFieldValueScoreElement(defaultHash, selectors.FieldValueScore{
			Field: field,
			Value: nil,
			Score: -1,
		})
	}()
	return ch
}

func (nop) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewKeysElement(defaultHash, make([]selectors.Key, 0))
	}()
	return ch
}

func (nop) Size(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewInt64Element(defaultHash, 0)
	}()
	return ch
}

func (nop) Members(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewFieldsElement(defaultHash, make([]selectors.Field, 0))
	}()
	return ch
}

func (nop) Score(selectors.Key, selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewPresenceElement(defaultHash, selectors.Presence{
			Inserted: false,
			Present:  false,
			Score:    -1,
		})
	}()
	return ch
}

func (nop) Hash() uint32 {
	return murmur3.Sum32([]byte("nop"))
}

func extractFields(members []selectors.FieldValueScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}
	return res
}
