package members

import (
	"io/ioutil"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/pkg/errors"
)

func TestBuilding(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(peerType, nodeName string,
			bindAddr string, bindPort int,
			advertiseAddr string, advertisePort int,
			existing []string,
			broadcastTime time.Duration,
		) bool {
			config, err := Build(
				WithPeerType(PeerType(peerType)),
				WithNodeName(nodeName),
				WithBindAddrPort(bindAddr, bindPort),
				WithAdvertiseAddrPort(advertiseAddr, advertisePort),
				WithExisting(existing),
				WithBroadcastTimeout(broadcastTime),
				WithLogOutput(ioutil.Discard),
			)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := nodeName, config.nodeName; expected != actual {
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

func TestPeerInfo(t *testing.T) {
	t.Parallel()

	t.Run("encode", func(t *testing.T) {
		fn := func(name, peerType, addr string, port int) bool {
			m := encodePeerInfoTag(PeerInfo{
				Name:    name,
				Type:    PeerType(peerType),
				APIAddr: addr,
				APIPort: port,
			})

			p, err := strconv.Atoi(m["api_port"])
			if err != nil {
				t.Fatal(err)
			}

			return m["name"] == name &&
				m["type"] == peerType &&
				m["api_addr"] == addr &&
				p == port
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("decode", func(t *testing.T) {
		fn := func(name, peerType, addr string, port int) bool {
			m := encodePeerInfoTag(PeerInfo{
				Name:    name,
				Type:    PeerType(peerType),
				APIAddr: addr,
				APIPort: port,
			})

			info, err := decodePeerInfoTag(m)
			if err != nil {
				t.Fatal(err)
			}

			return info.Name == name &&
				info.Type.String() == peerType &&
				info.APIAddr == addr &&
				info.APIPort == port
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("decode type failure", func(t *testing.T) {
		_, err := decodePeerInfoTag(map[string]string{
			"api_port": "1",
			"api_addr": "x",
		})

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("decode api_addr failure", func(t *testing.T) {
		_, err := decodePeerInfoTag(map[string]string{
			"type":     "x",
			"api_port": "1",
		})

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("decode api_port failure", func(t *testing.T) {
		_, err := decodePeerInfoTag(map[string]string{
			"type":     "x",
			"api_addr": "y",
		})

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("decode api_port integer failure", func(t *testing.T) {
		_, err := decodePeerInfoTag(map[string]string{
			"type":     "x",
			"api_addr": "y",
			"api_port": "x",
		})

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}
