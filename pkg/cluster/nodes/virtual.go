package nodes

import (
	"github.com/spaolacci/murmur3"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/SimonRichardson/coherence/pkg/store"
)

type virtual struct {
	hash  uint32
	store store.Store
}

// NewVirtual creates a local storage
func NewVirtual(store store.Store) Node {
	return &virtual{
		hash:  murmur3.Sum32([]byte("virtual")),
		store: store,
	}
}

func (v *virtual) Insert(key selectors.Key, members []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		changeSet, err := v.store.Insert(key, members)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewChangeSetElement(changeSet)
	}()
	return ch
}

func (v *virtual) Delete(key selectors.Key, members []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		changeSet, err := v.store.Delete(key, members)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewChangeSetElement(changeSet)
	}()
	return ch
}

func (v *virtual) Select(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		member, err := v.store.Select(key, field)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewFieldValueScoreElement(member)
	}()
	return ch
}

func (v *virtual) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		keys, err := v.store.Keys()
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewKeysElement(keys)
	}()
	return ch
}

func (v *virtual) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		size, err := v.store.Size(key)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewInt64Element(size)
	}()
	return ch
}

func (v *virtual) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		members, err := v.store.Members(key)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewFieldsElement(members)
	}()
	return ch
}

func (v *virtual) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)

		score, err := v.store.Score(key, field)
		if err != nil {
			ch <- selectors.NewErrorElement(err)
			return
		}
		ch <- selectors.NewPresenceElement(score)
	}()
	return ch
}

func (v *virtual) Hash() uint32 {
	return v.hash
}
