package nodes

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

func (nop) Insert(selectors.Key, []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (nop) Delete(selectors.Key, []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (nop) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewKeysElement(make([]selectors.Key, 0))
	return ch
}

func (nop) Size(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (nop) Members(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewFieldsElement(make([]selectors.Field, 0))
	return ch
}

func (nop) Repair([]selectors.KeyField) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}
