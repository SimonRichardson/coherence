package selectors

import (
	"encoding/json"
)

type Key string

func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(k))
}

func (k *Key) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	(*k) = Key(s)
	return err
}

func (k Key) Hash() uint32 {
	return murmur3.Sum32([]byte(k))
}

func (k Key) String() string {
	return string(k)
}

type Field string

func (f Field) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(f))
}

func (f *Field) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	(*f) = Field(s)
	return err
}

func (f Field) String() string {
	return string(f)
}

type KeyField struct {
	Key   Key   `json:"key"`
	Field Field `json:"field"`
}

type FieldScore struct {
	Field Field   `json:"field"`
	Score float64 `json:"score"`
}

type ChangeSet struct {
	Success int `json:"success"`
	Failure int `json:"failure"`
}

func (c ChangeSet) Equal(v ChangeSet) bool {
	return c.Success == v.Success && c.Failure == v.Failure
}
