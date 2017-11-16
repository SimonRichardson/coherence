package cache

import (
	"testing"
	"testing/quick"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/harness/generators"
)

func TestBuildingCache(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(name string) bool {
			config, err := Build(
				With(name),
			)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := name, config.name; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid build", func(t *testing.T) {
		_, err := Build(
			func(config *Config) error {
				return errors.Errorf("bad")
			},
		)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("nop", func(t *testing.T) {
		config, err := Build(
			With("nop"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config, log.NewNopLogger())
		if err != nil {
			t.Error(err)
		}
	})
}

func TestRequiresRemoteConfig(t *testing.T) {
	t.Parallel()

	t.Run("remote", func(t *testing.T) {
		got := RequiresRemoteConfig("remote")
		if expected, actual := true, got; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("any", func(t *testing.T) {
		fn := func(name generators.ASCII) bool {
			return !RequiresRemoteConfig(name.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
