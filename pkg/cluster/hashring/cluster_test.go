package hashring

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/trussle/harness/generators"

	apiMocks "github.com/SimonRichardson/coherence/pkg/api/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/members"
	"github.com/SimonRichardson/coherence/pkg/cluster/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	nodeMocks "github.com/SimonRichardson/coherence/pkg/cluster/nodes/mocks"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/spaolacci/murmur3"
)

func TestClusterRead(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		strategy := apiMocks.NewMockTransportStrategy(ctrl)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestClusterWrite(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		strategy := apiMocks.NewMockTransportStrategy(ctrl)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		nodes, _ := cluster.Write(selectors.Key("a"), selectors.Strong)

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		a, b := murmur3.Sum32([]byte("0.0.0.0:8080")), murmur3.Sum32([]byte("0.0.0.0:8081"))

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(a)
		transport.EXPECT().Hash().Return(b)

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		want := []uint32{
			a,
			b,
		}

		nodes, finish := cluster.Write(selectors.Key("a"), selectors.Strong)
		if expected, actual := want, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := true, finish(want) == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("updateRemoteActors twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		a, b := murmur3.Sum32([]byte("0.0.0.0:8080")), murmur3.Sum32([]byte("0.0.0.0:8081"))

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(a)
		transport.EXPECT().Hash().Return(b)

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		want := []uint32{
			a,
			b,
		}

		nodes, finish := cluster.Write(selectors.Key("a"), selectors.Strong)
		if expected, actual := want, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := true, finish(want) == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestDispatchBloomEvent(t *testing.T) {
	t.Parallel()

	t.Run("dispatch - cached", func(t *testing.T) {
		fn := func(hash uint32, name generators.ASCII) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash).Times(2)

			actor := NewActor(func() nodes.Node {
				return node
			})

			peer := mocks.NewMockPeer(ctrl)

			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.actors.Set(actor)
			cluster.times[hash] = actor.clock.Now()

			cluster.dispatchBloomEvent(hash)

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dispatch - actors", func(t *testing.T) {
		fn := func(hash uint32, name generators.ASCII) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash).Times(2)

			actor := NewActor(func() nodes.Node {
				return node
			})

			buf := new(bytes.Buffer)
			if _, err := actor.bloom.Write(buf); err != nil {
				t.Fatal(err)
			}

			payload, err := json.Marshal(bloomEventPayload{
				Name:  name.String(),
				Hash:  hash,
				Bloom: buf.Bytes(),
			})
			if err != nil {
				t.Fatal(err)
			}

			peer := mocks.NewMockPeer(ctrl)
			peer.EXPECT().Name().Return(name.String())
			peer.EXPECT().DispatchEvent(members.NewUserEvent(BloomEventType, payload))

			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.actors.Set(actor)

			cluster.dispatchBloomEvent(hash)

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dispatch - no actors", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.dispatchBloomEvent(hash)

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestUpdateRemoteActors(t *testing.T) {
	t.Parallel()

	t.Run("update", func(t *testing.T) {
		fn := func(host string) bool {
			hosts := []string{host}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			for _, v := range hosts {
				if ok := cluster.ring.Add(v); !ok {
					t.Fatalf("expected valid %v %s", cluster.ring.Hosts(), v)
				}
			}
			return cluster.updateRemoteActors(hosts) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("update - remove old", func(t *testing.T) {
		fn := func(host string, old string) bool {
			if host == old {
				return true
			}

			hosts := []string{host}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.ring.Add(old)
			for _, v := range hosts {
				if ok := cluster.ring.Add(v); !ok {
					t.Fatalf("expected valid %v %s", cluster.ring.Hosts(), v)
				}
			}
			return cluster.updateRemoteActors(hosts) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFilter(t *testing.T) {
	t.Parallel()

	t.Run("filter - match", func(t *testing.T) {
		fn := func(host string) bool {
			hosts := []string{host}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash(hosts[0]))

			actor := NewActor(func() nodes.Node {
				return node
			})
			actor.bloom.Add(hosts[0])

			actors := NewActors()
			actors.Set(actor)

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.actors = actors
			return len(cluster.filter(hosts, hosts[0])) == 1
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("filter - no match", func(t *testing.T) {
		fn := func(hosts []string, key string) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			return len(cluster.filter(hosts, key)) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestActorTimeIsIncremented(t *testing.T) {
	t.Parallel()

	t.Run("valid - no increment", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			return cluster.actorTimeIncremented(actor)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("valid - increment", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.times[hash] = actor.clock.Now()
			actor.clock.Increment()
			return cluster.actorTimeIncremented(actor)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.times[hash] = actor.clock.Now()
			return !cluster.actorTimeIncremented(actor)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestStoreActorTime(t *testing.T) {
	t.Parallel()

	t.Run("match", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := nodeMocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			actors := NewActors()
			actors.Set(actor)

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.actors = actors

			cluster.storeActorTime(hash)
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("no match", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			actors := NewActors()

			peer := mocks.NewMockPeer(ctrl)
			strategy := apiMocks.NewMockTransportStrategy(ctrl)

			cluster := NewCluster(peer, strategy, 4, "", log.NewNopLogger())
			cluster.actors = actors

			cluster.storeActorTime(hash)
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func extractAddresses(nodes []nodes.Node) []uint32 {
	res := make([]uint32, 0)
	for _, v := range nodes {
		res = append(res, v.Hash())
	}
	return res
}

func match(a, b []uint32) bool {
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i] < b[j]
	})
	return reflect.DeepEqual(a, b)
}
