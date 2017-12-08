package selectors

type errNotFound struct {
	err error
}

// NewNotFoundError creates a new NotFoundError
func NewNotFoundError(err error) error {
	return errNotFound{err}
}

func (e errNotFound) Error() string {
	return e.err.Error()
}

// NotFoundError finds if the error passed in, is actually a partial error or not
func NotFoundError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(errNotFound)
	return ok
}
