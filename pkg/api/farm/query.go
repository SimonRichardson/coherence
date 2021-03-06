package farm

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/pkg/errors"
)

const (
	defaultContentType = "application/json"
)

// KeyParams represents a parameter that has a selectors.Key
type KeyParams interface {
	Key() selectors.Key
}

// FieldParams represents a object that has a selectors.Field
type FieldParams interface {
	Field() selectors.Field
}

// KeyQueryParams defines all the dimensions of a query.
type KeyQueryParams struct {
	key    selectors.Key
	quorum selectors.Quorum
}

// Key returns the key value from the parameters
func (qp KeyQueryParams) Key() selectors.Key {
	return qp.key
}

// DecodeFrom populates a KeyQueryParams from a URL.
func (qp *KeyQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if rb == queryRequired {
		if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
			return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
		}
	}

	key := u.Query().Get("key")
	if key == "" {
		return errors.Errorf("expected 'key' but got %q", key)
	}
	qp.key = selectors.Key(key)

	var (
		err    error
		quorum = u.Query().Get("quorum")
	)
	if quorum != "" {
		if qp.quorum, err = selectors.ParseQuorum(quorum); err != nil {
			return errors.Errorf("expected 'quorum' but got %q", quorum)
		}
	} else {
		qp.quorum = selectors.Strong
	}

	return nil
}

// KeyFieldQueryParams defines all the dimensions of a query.
type KeyFieldQueryParams struct {
	key    selectors.Key
	field  selectors.Field
	quorum selectors.Quorum
}

// Key returns the key value from the parameters
func (qp KeyFieldQueryParams) Key() selectors.Key {
	return qp.key
}

// Field returns the field value from the parameters
func (qp KeyFieldQueryParams) Field() selectors.Field {
	return qp.field
}

// DecodeFrom populates a KeyFieldQueryParams from a URL.
func (qp *KeyFieldQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if rb == queryRequired {
		if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
			return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
		}
	}

	key := u.Query().Get("key")
	if key == "" {
		return errors.Errorf("expected 'key' but got %q", key)
	}
	qp.key = selectors.Key(key)

	field := u.Query().Get("field")
	if field == "" {
		return errors.Errorf("expected 'field' but got %q", field)
	}
	qp.field = selectors.Field(field)

	var (
		err    error
		quorum = u.Query().Get("quorum")
	)
	if quorum != "" {
		if qp.quorum, err = selectors.ParseQuorum(quorum); err != nil {
			return errors.Errorf("expected 'quorum' but got %q", quorum)
		}
	} else {
		qp.quorum = selectors.Strong
	}

	return nil
}

type queryBehavior int

const (
	queryRequired queryBehavior = iota
	queryOptional
)
