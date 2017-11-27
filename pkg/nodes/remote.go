package nodes

import (
	"encoding/json"
	"fmt"

	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/selectors"
)

type remoteNode interface {

	// Host returns the value of the remote node
	Host() string
}

type remote struct {
	client *client.Client
}

// NewRemote creates a Node that communicates with a remote service
func NewRemote(client *client.Client) Node {
	return &remote{
		client: client,
	}
}

func (r *remote) Insert(key selectors.Key, fields []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.write(key, "insert", fields, ch)
	}()
	return ch
}

func (r *remote) Delete(key selectors.Key, fields []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.write(key, "delete", fields, ch)
	}()
	return ch
}

func (r *remote) Select(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.read(key, field, ch)
	}()
	return ch
}

func (r *remote) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.readKeys(ch)
	}()
	return ch
}

func (r *remote) Size(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.readSize(key, ch)
	}()
	return ch
}

func (r *remote) Members(key selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.readMembers(key, ch)
	}()
	return ch
}

func (r *remote) Score(key selectors.Key, field selectors.Field) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go func() {
		defer close(ch)
		r.readScore(key, field, ch)
	}()
	return ch
}

// Host returns the client host
func (r *remote) Host() string {
	return r.client.Host()
}

func (r *remote) write(key selectors.Key, path string, fields []selectors.FieldScore, dst chan<- selectors.Element) {
	b, err := json.Marshal(struct {
		Members []selectors.FieldScore `json:"members"`
	}{
		Members: fields,
	})
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	res, err := r.client.Post(fmt.Sprintf("/store/%s?key=%s", path, key.String()), b)
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

func (r *remote) read(key selectors.Key, field selectors.Field, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/store/select?key=%s&field=%s", key.String(), field.String()))
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var fieldScore struct {
		Records selectors.FieldScore `json:"records"`
	}
	if err := json.Unmarshal(res, &fieldScore); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewFieldScoreElement(fieldScore.Records)
}

func (r *remote) readKeys(dst chan<- selectors.Element) {
	res, err := r.client.Get("/store/keys")
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

func (r *remote) readSize(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/store/size?key=%s", key.String()))
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

func (r *remote) readMembers(key selectors.Key, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/store/members?key=%s", key.String()))
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

func (r *remote) readScore(key selectors.Key, field selectors.Field, dst chan<- selectors.Element) {
	res, err := r.client.Get(fmt.Sprintf("/store/score?key=%s&field=%s", key.String(), field.String()))
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
