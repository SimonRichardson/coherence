package nodes

import "github.com/trussle/coherence/pkg/selectors"

type virtual struct {
	store Store
}

func (v *virtual) Insert(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		var changeSet selectors.ChangeSet
		for _, member := range members {
			if v.store.Insert(key, member) {
				changeSet.Success++
			} else {
				changeSet.Failure++
			}
		}
		ch <- selectors.NewChangeSetElement(changeset)
	}()
	return ch
}

func (v *virtual) Delete(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		var changeSet selectors.ChangeSet
		for _, member := range members {
			if v.store.Insert(key, member) {
				changeSet.Success++
			} else {
				changeSet.Failure++
			}
		}
		ch <- selectors.NewChangeSetElement(changeset)
	}()
	return ch
}

func (v *virtual) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewKeysElement(make([]selectors.Key, 0))
	return ch
}

func (v *virtual) Size(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (v *virtual) Members(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewFieldsElement(make([]selectors.Field, 0))
	return ch
}

func (v *virtual) Repair([]selectors.KeyField) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}
