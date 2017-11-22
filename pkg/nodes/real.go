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
	go r.write(key, fields, ch)
	return ch
}

func (r *real) Delete(key selectors.Key, fields []selectors.FieldScore) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	go r.write(key, fields, ch)
	return ch
}

func (r *real) Keys() <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewKeysElement(make([]selectors.Key, 0))
	return ch
}

func (r *real) Size(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (r *real) Members(selectors.Key) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewFieldsElement(make([]selectors.Field, 0))
	return ch
}

func (r *real) Repair([]selectors.KeyField) <-chan selectors.Element {
	ch := make(chan selectors.Element)
	ch <- selectors.NewIntElement(0)
	return ch
}

func (r *real) write(key selectors.Key, fields []selectors.FieldScore, dst chan<- selectors.Element) {
	b, err := json.Marshal(struct {
		Data []selectors.FieldScore `json:"data"`
	}{
		Data: fields,
	})
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	res, err := r.client.Post(fmt.Sprintf("/insert?key=%s", key.String()), b)
	if err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	var result struct {
		Result int `json:"result"`
	}
	if err := json.Unmarshal(res, &result); err != nil {
		dst <- selectors.NewErrorElement(err)
		return
	}

	dst <- selectors.NewIntElement(result.Result)
}
