package nodes

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

func (nop) Insert(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
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
		ch <- selectors.NewChangeSetElement(selectors.ChangeSet{
			Success: make([]selectors.Field, 0),
			Failure: extractFields(members),
		})
	}()
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

func extractFields(members []selectors.FieldScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}
	return res
}