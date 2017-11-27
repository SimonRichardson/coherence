package nodes

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

// NewNop creates a nop Node
func NewNop() Node {
	return nop{}
}

func (nop) Insert(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewChangeSetElement(selectors.ChangeSet{
			Success: make([]selectors.Field, 0),
			Failure: extractFields(members),
		})
	}()
	return ch
}

func (nop) Delete(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewChangeSetElement(selectors.ChangeSet{
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

		ch <- selectors.NewFieldScoreElement(selectors.FieldScore{
			Field: field,
			Score: -1,
		})
	}()
	return ch
}

func (nop) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewKeysElement(make([]selectors.Key, 0))
	}()
	return ch
}

func (nop) Size(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewInt64Element(0)
	}()
	return ch
}

func (nop) Members(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewFieldsElement(make([]selectors.Field, 0))
	}()
	return ch
}

func (nop) Score(selectors.Key, selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		ch <- selectors.NewPresenceElement(selectors.Presence{})
	}()
	return ch
}

func extractFields(members []selectors.FieldScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}
	return res
}
