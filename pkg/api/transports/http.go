package transports

import (
	"encoding/json"
	"fmt"

	"github.com/spaolacci/murmur3"
	"github.com/trussle/coherence/pkg/api"
	"github.com/trussle/coherence/pkg/api/client"
	"github.com/trussle/coherence/pkg/selectors"
)

type httpTransport struct {
	hash   uint32
	client *client.Client
}

// NewHTTPTransport creates a new Transport that uses the HTTP protocol
func NewHTTPTransport(client *client.Client) api.Transport {
	return &httpTransport{
		hash:   murmur3.Sum32([]byte(client.Host())),
		client: client,
	}
}

func (t *httpTransport) Insert(key selectors.Key, fields []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	return t.write("insert", key, fields)
}

func (t *httpTransport) Delete(key selectors.Key, fields []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	return t.write("delete", key, fields)
}

func (t *httpTransport) Select(key selectors.Key, field selectors.Field) (record selectors.FieldValueScore, err error) {
	var res []byte
	res, err = t.client.Get(fmt.Sprintf("/store/select?key=%s&field=%s", key.String(), field.String()))
	if err != nil {
		return
	}

	var fieldValueScore struct {
		Records selectors.FieldValueScore `json:"records"`
	}
	if err = json.Unmarshal(res, &fieldValueScore); err != nil {
		return
	}

	record = fieldValueScore.Records
	return
}

func (t *httpTransport) Keys() (record []selectors.Key, err error) {
	var res []byte
	res, err = t.client.Get("/store/keys")
	if err != nil {
		return
	}

	var keys struct {
		Records []selectors.Key `json:"records"`
	}
	if err = json.Unmarshal(res, &keys); err != nil {
		return
	}

	record = keys.Records
	return
}

func (t *httpTransport) Size(key selectors.Key) (record int64, err error) {
	var res []byte
	res, err = t.client.Get(fmt.Sprintf("/store/size?key=%s", key.String()))
	if err != nil {
		return
	}

	var size struct {
		Records int64 `json:"records"`
	}
	if err = json.Unmarshal(res, &size); err != nil {
		return
	}

	record = size.Records
	return
}

func (t *httpTransport) Members(key selectors.Key) (record []selectors.Field, err error) {
	var res []byte
	res, err = t.client.Get(fmt.Sprintf("/store/members?key=%s", key.String()))
	if err != nil {
		return
	}

	var members struct {
		Records []selectors.Field `json:"records"`
	}
	if err = json.Unmarshal(res, &members); err != nil {
		return
	}

	record = members.Records
	return
}

func (t *httpTransport) Score(key selectors.Key, field selectors.Field) (record selectors.Presence, err error) {
	var res []byte
	res, err = t.client.Get(fmt.Sprintf("/store/score?key=%s&field=%s", key.String(), field.String()))
	if err != nil {
		return
	}

	var score struct {
		Records selectors.Presence `json:"records"`
	}
	if err = json.Unmarshal(res, &score); err != nil {
		return
	}

	record = score.Records
	return
}

func (t *httpTransport) write(path string, key selectors.Key, fields []selectors.FieldValueScore) (record selectors.ChangeSet, err error) {
	var b []byte
	b, err = json.Marshal(struct {
		Members []selectors.FieldValueScore `json:"members"`
	}{
		Members: fields,
	})
	if err != nil {
		return
	}

	var res []byte
	res, err = t.client.Post(fmt.Sprintf("/store/%s?key=%s", path, key.String()), b)
	if err != nil {
		return
	}

	var changeset struct {
		Records selectors.ChangeSet `json:"records"`
	}
	if err = json.Unmarshal(res, &changeset); err != nil {
		return
	}

	record = changeset.Records
	return
}

func (t *httpTransport) Hash() uint32 {
	return t.hash
}
