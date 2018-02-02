package api

import "github.com/SimonRichardson/coherence/pkg/selectors"

// MembersInput defines a simple type for marshalling and unmarshalling members
type MembersInput struct {
	Members []FieldValueScore `json:"members"`
}

// Key is an input for marshalling json input and output from the api
type Key string

func KeysOutput(a []selectors.Key) []Key {
	res := make([]Key, len(a))
	for k, v := range a {
		res[k] = Key(v.String())
	}
	return res
}

// Field is an input for marshalling json input and output from the api
type Field string

func FieldsOutput(a []selectors.Field) []Field {
	res := make([]Field, len(a))
	for k, v := range a {
		res[k] = Field(v.String())
	}
	return res
}

// FieldValueScore is an input for marshalling json input and out from the api
type FieldValueScore struct {
	Field Field  `json:"field"`
	Value []byte `json:"value"`
	Score int64  `json:"score"`
}

// FieldScore is an input for marshalling json input and out from the api
type FieldScore struct {
	Field Field `json:"field"`
	Score int64 `json:"score"`
}

// Presence is an input for marshalling json input and out from the api
type Presence struct {
	Present  bool  `json:"present"`
	Inserted bool  `json:"inserted"`
	Score    int64 `json:"score"`
}

// ChangeSet is an input for marshalling json input and out from the api
type ChangeSet struct {
	Success []Field `json:"success"`
	Failure []Field `json:"failure"`
}

func ChangeSetOutput(a selectors.ChangeSet) ChangeSet {
	return ChangeSet{
		Success: FieldsOutput(a.Success),
		Failure: FieldsOutput(a.Failure),
	}
}
