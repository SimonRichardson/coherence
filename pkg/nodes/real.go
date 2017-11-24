package nodes

import (
	"encoding/json"
	"fmt"

	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/selectors"
)

type real struct {
	client *client.Client
}

func (r *real) Insert(key selectors.Key, fields []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.write(key, "insert", fields, ch)
	return ch
}

func (r *real) Delete(key selectors.Key, fields []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.write(key, "delete", fields, ch)
	return ch
}

func (r *real) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.readKeys(ch)
	return ch
}

func (r *real) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.readSize(key, ch)
	return ch
}

func (r *real) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.readMembers(key, ch)
	return ch
}

func (r *real) Repair([]selectors.KeyField) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (r *real) write(key selectors.Key, path string, fields []selectors.FieldScore, dst chan<- selectors.Element) {
	b, err := json.Marshal(struct {
		Members []selectors.FieldScore `json:"members"`
	}{
		Members: fields,
	})
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	res, err := r.client.Post(fmt.Sprintf("/%s?key=%s", path, key.String()), b)
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var changeset selectors.ChangeSet
	if err := json.Unmarshal(res, &changeset); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewChangeSetElement(changeset)
}

func (r *real) readKeys(dst chan<- selectors.Element) {
	res, err := r.client.Get("/keys")
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var keys []selectors.Key
	if err := json.Unmarshal(res, &keys); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewKeysElement(keys)
}

func (r *real) readSize(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/size?key=%s", key.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var size int
	if err := json.Unmarshal(res, &size); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewIntElement(size)
}

func (r *real) readMembers(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/members?key=%s", key.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var members []selectors.Field
	if err := json.Unmarshal(res, &members); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewFieldsElement(members)
}
