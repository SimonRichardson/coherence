package members

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
)

func TestMembersEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		var (
			members = []Member{nopMember{}}
			evt     = NewMemberEvent(EventMemberJoined, members)
			real    = evt.(*MemberEvent)
		)

		if expected, actual := EventMemberJoined, real.EventType; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := members, real.Members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := EventMember, real.Type(); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestUserEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string, b []byte) bool {
			var (
				evt  = NewUserEvent(a, b)
				real = evt.(*UserEvent)
			)

			if expected, actual := a, real.Name; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := b, real.Payload; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := EventUser, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestQueryEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string, b []byte) bool {
			var (
				evt  = NewQueryEvent(a, b, nil)
				real = evt.(*QueryEvent)
			)

			if expected, actual := a, real.Name; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := b, real.Payload; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := EventQuery, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestErrorEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string) bool {
			var (
				evt  = NewErrorEvent(errors.Errorf("%s", a))
				real = evt.(*ErrorEvent)
			)

			if expected, actual := a, real.Error.Error(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := EventError, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
