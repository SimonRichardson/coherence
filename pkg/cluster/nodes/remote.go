package nodes

import (
	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/selectors"
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
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewChangeSetElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Delete(key selectors.Key, fields []selectors.FieldValueScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Delete(key, fields); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewChangeSetElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Select(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Select(key, field); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewFieldValueScoreElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Keys(); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewKeysElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Size(key); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewInt64Element(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Members(key); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewFieldsElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		if value, err := r.transport.Score(key, field); err != nil {
			ch <- selectors.NewErrorElement(r.hash, err)
		} else {
			ch <- selectors.NewPresenceElement(r.hash, value)
		}
	}()
	return ch
}

func (r *remote) Hash() uint32 {
	return r.hash
}
