package nodes

import (
	"github.com/trussle/coherence/pkg/api"
	"github.com/trussle/coherence/pkg/selectors"
)

type remote struct {
	hash      uint32
	transport api.Transport
}

// NewRemote creates a Node that communicates with a remote service
func NewRemote(transport api.Transport) Node {
	return &remote{
		hash:      transport.Hash(),
		transport: transport,
	}
}

func (r *remote) Insert(key selectors.Key, fields []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Insert(key, fields); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewChangeSetElement(value)
		}
	}()
	return ch
}

func (r *remote) Delete(key selectors.Key, fields []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Delete(key, fields); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewChangeSetElement(value)
		}
	}()
	return ch
}

func (r *remote) Select(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Select(key, field); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewFieldValueScoreElement(value)
		}
	}()
	return ch
}

func (r *remote) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Keys(); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewKeysElement(value)
		}
	}()
	return ch
}

func (r *remote) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Size(key); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewInt64Element(value)
		}
	}()
	return ch
}

func (r *remote) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Members(key); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewFieldsElement(value)
		}
	}()
	return ch
}

func (r *remote) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Score(key, field); err != nil {
			ch <- selectors.NewErrorElement(err)
		} else {
			ch <- selectors.NewPresenceElement(value)
		}
	}()
	return ch
}

func (r *remote) Hash() uint32 {
	return r.hash
}
