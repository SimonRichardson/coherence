package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/SimonRichardson/flagset"
	"github.com/SimonRichardson/gexec"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/trussle/coherence/pkg/api"
	"github.com/trussle/coherence/pkg/api/transports"
	"github.com/trussle/coherence/pkg/cluster"
	"github.com/trussle/coherence/pkg/farm"
	"github.com/trussle/coherence/pkg/hashring"
	"github.com/trussle/coherence/pkg/members"
	"github.com/trussle/coherence/pkg/status"
	"github.com/trussle/coherence/pkg/store"
)

const (
	defaultCacheSize              = 1000
	defaultCacheBuckets           = 10
	defaultCacheReplicationFactor = 2
	defaultNodeReplicationFactor  = 3
	defaultMetricsRegistration    = true
	defaultTransportProtocol      = "http"
)

func runCache(args []string) error {
	// flags for the cache command
	var (
		flags = flagset.NewFlagSet("cache", flag.ExitOnError)

		debug                  = flags.Bool("debug", false, "debug logging")
		apiAddr                = flags.String("api", defaultAPIAddr, "listen address for query API")
		clusterBindAddr        = flags.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr   = flags.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		cacheSize              = flags.Uint("cache.size", defaultCacheSize, "number items the cache should hold")
		cacheBuckets           = flags.Uint("cache.buckets", defaultCacheBuckets, "number of buckets to use with the cache")
		cacheReplicationFactor = flags.Int("cache.replication.factor", defaultCacheReplicationFactor, "replication factor for remote configuration")
		nodeReplicationFactor  = flags.Int("node.replication.factor", defaultNodeReplicationFactor, "replication factor for node configuration")
		transportProtocol      = flags.String("transport.protocol", defaultTransportProtocol, "protocol used to talk to remote nodes (http)")
		metricsRegistration    = flags.Bool("metrics.registration", defaultMetricsRegistration, "Registration of metrics on launch")
		clusterPeers           = stringslice{}
	)

	flags.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flags.Usage = usageFor(flags, "cache [flags]")
	if err := flags.Parse(args); err != nil {
		return nil
	}

	// Setup the logger.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if *debug {
			logLevel = level.AllowAll()
		}
		logger = log.NewLogfmtLogger(os.Stdout)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = level.NewFilter(logger, logLevel)
	}

	// Instrumentation
	connectedClients := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "coherence",
		Name:      "connected_clients",
		Help:      "Number of currently connected clients by modality.",
	}, []string{"modality"})
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "coherence",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})

	if *metricsRegistration {
		prometheus.MustRegister(
			connectedClients,
			apiDuration,
		)
	}

	apiNetwork, apiAddress, err := parseAddr(*apiAddr, defaultAPIPort)
	if err != nil {
		return err
	}
	apiListener, err := net.Listen(apiNetwork, apiAddress)
	if err != nil {
		return err
	}
	level.Debug(logger).Log("API", fmt.Sprintf("%s://%s", apiNetwork, apiAddress))

	apiAddress, apiPort, err := parseClusterAddr(*apiAddr, defaultAPIPort)
	if err != nil {
		return err
	}

	peer, err := configureRemoteCache(logger,
		*cacheReplicationFactor,
		apiAddress, apiPort,
		*clusterBindAddr,
		*clusterAdvertiseAddr,
		clusterPeers.Slice(),
	)
	if err != nil {
		return err
	}

	transport, err := transports.Parse(*transportProtocol)
	if err != nil {
		return err
	}

	var (
		persistence = store.New(*cacheBuckets, *cacheSize, log.With(logger, "component", "store"))
		nodeSet     = hashring.NewNodeSet(peer, transport, *nodeReplicationFactor, log.With(logger, "component", "nodeset"))
		supervisor  = farm.NewReal(nodeSet)
	)

	// Execution group.
	g := gexec.NewGroup()
	gexec.Block(g)
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			if _, err := peer.Join(); err != nil {
				return err
			}
			<-cancel
			return peer.Leave()
		}, func(error) {
			close(cancel)
		})
	}
	{
		g.Add(func() error {
			nodeSet.Listen(func(reason hashring.Reason) {
				level.Debug(logger).Log("component", "nodeset", "reason", reason.String())
			})
			return nodeSet.Run()
		}, func(error) {
			nodeSet.Stop()
		})
	}
	{
		g.Add(func() error {
			storeAPI := api.NewAPI(
				persistence,
				log.With(logger, "component", "store_api"),
				connectedClients.WithLabelValues("api"),
				apiDuration,
			)
			defer storeAPI.Close()

			mux := http.NewServeMux()
			mux.Handle("/store/", http.StripPrefix("/store", storeAPI))
			mux.Handle("/cache/", http.StripPrefix("/cache", api.NewAPI(
				supervisor,
				log.With(logger, "component", "cache_api"),
				connectedClients.WithLabelValues("api"),
				apiDuration,
			)))
			mux.Handle("/status/", http.StripPrefix("/status", status.NewAPI(
				supervisor,
				log.With(logger, "component", "status_api"),
				connectedClients.WithLabelValues("status"),
				apiDuration,
			)))

			registerMetrics(mux)
			registerProfile(mux)

			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	gexec.Interrupt(g)
	return g.Run()
}

func configureRemoteCache(logger log.Logger,
	replicationFactor int,
	apiAddr string, apiPort int,
	bindAddr, advertiseAddr string,
	peers []string,
) (cluster.Peer, error) {
	clusterBindHost, clusterBindPort, err := parseClusterAddr(bindAddr, defaultClusterPort)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("cluster_bind", fmt.Sprintf("%s:%d", clusterBindHost, clusterBindPort))

	var (
		clusterAdvertiseHost string
		clusterAdvertisePort int
	)
	if advertiseAddr != "" {
		clusterAdvertiseHost, clusterAdvertisePort, err = parseClusterAddr(advertiseAddr, defaultClusterPort)
		if err != nil {
			return nil, err
		}
		level.Info(logger).Log("cluster_advertise", fmt.Sprintf("%s:%d", clusterAdvertiseHost, clusterAdvertisePort))
	}

	// Safety warning.
	if addr, err := cluster.CalculateAdvertiseAddress(clusterBindHost, clusterAdvertiseHost); err != nil {
		level.Warn(logger).Log("err", "couldn't deduce an advertise address: "+err.Error())
	} else if hasNonlocal(peers) && isUnroutable(addr.String()) {
		level.Warn(logger).Log("err", "this node advertises itself on an unroutable address", "addr", addr.String())
		level.Warn(logger).Log("err", "this node will be unreachable in the cluster")
		level.Warn(logger).Log("err", "provide -cluster.advertise-addr as a routable IP address or hostname")
	}

	cacheMembersConfig, err := members.Build(
		members.WithPeerType(cluster.PeerTypeStore),
		members.WithNodeName(uuid.New()),
		members.WithAPIAddrPort(apiAddr, apiPort),
		members.WithBindAddrPort(clusterBindHost, clusterBindPort),
		members.WithAdvertiseAddrPort(clusterAdvertiseHost, clusterAdvertisePort),
		members.WithExisting(peers),
		members.WithLogOutput(membersLogOutput{
			logger: log.With(logger, "component", "cluster"),
		}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "members remote config")
	}

	cacheMembers, err := members.NewRealMembers(cacheMembersConfig, log.With(logger, "component", "members"))
	if err != nil {
		return nil, errors.Wrap(err, "members remote")
	}

	return cluster.NewPeer(cacheMembers, log.With(logger, "component", "peer")), nil
}

type membersLogOutput struct {
	logger log.Logger
}

func (m membersLogOutput) Write(b []byte) (int, error) {
	level.Debug(m.logger).Log("fwd_msg", string(b))
	return len(b), nil
}
