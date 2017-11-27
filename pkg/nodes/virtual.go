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
				changeSet.Success = append(changeSet.Success, member.Field)
			} else {
				changeSet.Failure = append(changeSet.Failure, member.Field)
			}
		}
		ch <- selectors.NewChangeSetElement(changeSet)
	}()
	return ch
}

func (v *virtual) Delete(key selectors.Key, members []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		var changeSet selectors.ChangeSet
		for _, member := range members {
			if v.store.Delete(key, member) {
				changeSet.Success = append(changeSet.Success, member.Field)
			} else {
				changeSet.Failure = append(changeSet.Failure, member.Field)
			}
		}
		ch <- selectors.NewChangeSetElement(changeSet)
	}()
	return ch
}

func (v *virtual) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		ch <- selectors.NewKeysElement(v.store.Keys())
	}()
	return ch
}

func (v *virtual) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		ch <- selectors.NewInt64Element(v.store.Size(key))
	}()
	return ch
}

func (v *virtual) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		ch <- selectors.NewFieldsElement(v.store.Members(key))
	}()
	return ch
}

func (v *virtual) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		ch <- selectors.NewPresenceElement(v.store.Score(key, field))
	}()
	return ch
}
