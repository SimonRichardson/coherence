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

func (r *real) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.readScore(key, field, ch)
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

	var changeset struct {
		Records selectors.ChangeSet `json:"records"`
	}
	if err := json.Unmarshal(res, &changeset); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewChangeSetElement(changeset.Records)
}

func (r *real) readKeys(dst chan<- selectors.Element) {
	res, err := r.client.Get("/keys")
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var keys struct {
		Records []selectors.Key `json:"records"`
	}
	if err := json.Unmarshal(res, &keys); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewKeysElement(keys.Records)
}

func (r *real) readSize(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/size?key=%s", key.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var size struct {
		Records int64 `json:"records"`
	}
	if err := json.Unmarshal(res, &size); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewInt64Element(size.Records)
}

func (r *real) readMembers(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/members?key=%s", key.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var members struct {
		Records []selectors.Field `json:"records"`
	}
	if err := json.Unmarshal(res, &members); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewFieldsElement(members.Records)
}

func (r *real) readScore(key selectors.Key, field selectors.Field, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/score?key=%s&field=%s", key.String(), field.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var score struct {
		Records selectors.Presence `json:"records"`
	}
	if err := json.Unmarshal(res, &score); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewPresenceElement(score.Records)
}
