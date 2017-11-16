// +build integration

package members

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/trussle/uuid"
)

func TestRealMembers_Integration(t *testing.T) {
	t.Parallel()

	config, err := Build(
		WithBindAddrPort("0.0.0.0", 8080),
		WithLogOutput(ioutil.Discard),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		members.Close()

		if expected, actual := false, members == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("join", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		defer members.Close()

		a, err := members.Join()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, a; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("leave", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		defer members.Close()

		a, err := members.Join()
		if err != nil {
			t.Fatal(err)
		}

		err = members.Leave()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, a; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("member list", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		if _, err = members.Join(); err != nil {
			t.Fatal(err)
		}

		m := members.MemberList()
		if expected, actual := false, m == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		if _, err = members.Join(); err != nil {
			t.Fatal(err)
		}

		var got []PeerInfo
		err = members.Walk(func(info PeerInfo) error {
			got = append(got, info)
			return nil
		})

		want := []PeerInfo{
			PeerInfo{Type: PeerType(""), APIAddr: "0.0.0.0", APIPort: 8080},
		}
		if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("close", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		err = members.Close()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestRealMemberList(t *testing.T) {
	t.Parallel()

	id, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}

	config, err := Build(
		WithBindAddrPort("0.0.0.0", 8081),
		WithLogOutput(ioutil.Discard),
		WithNodeName(id.String()),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("number of members", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		amount := members.MemberList().NumMembers()
		if expected, actual := 1, amount; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("local node", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		node := members.MemberList().LocalNode()
		if expected, actual := false, node == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("local node name", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		name := members.MemberList().LocalNode().Name()
		if expected, actual := id.String(), name; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("members", func(t *testing.T) {
		members, err := NewRealMembers(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer members.Close()

		m := members.MemberList().Members()
		if expected, actual := 1, len(m); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}
