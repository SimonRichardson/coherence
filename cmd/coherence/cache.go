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
	"github.com/trussle/coherence/pkg/cache"
	"github.com/trussle/coherence/pkg/cluster"
	"github.com/trussle/coherence/pkg/members"
	"github.com/trussle/coherence/pkg/status"
)

const (
	defaultCache               = "nop"
	defaultCacheSize           = 1000
	defaultReplicationFactor   = 2
	defaultMetricsRegistration = true
)

func runCache(args []string) error {
	// flags for the cache command
	var (
		flags = flagset.NewFlagSet("cache", flag.ExitOnError)

		debug                  = flags.Bool("debug", false, "debug logging")
		apiAddr                = flags.String("api", defaultAPIAddr, "listen address for query API")
		clusterBindAddr        = flags.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr   = flags.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		cacheType              = flags.String("cache", defaultCache, "type of temporary cache to use (remote, virtual, nop)")
		cacheSize              = flags.Int("cache.size", defaultCacheSize, "number items the cache should hold")
		cacheReplicationFactor = flags.Int("cache.replication.factor", defaultReplicationFactor, "replication factor for remote configuration")
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

	// Make sure that we need the remote config, before going a head and setting
	// it up. It prevents allocations that aren't required.
	var remoteCacheConfig *cache.RemoteConfig
	if cache.RequiresRemoteConfig(*cacheType) {
		remoteCacheConfig, err = configureRemoteCache(logger,
			*cacheReplicationFactor,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			clusterPeers.Slice(),
		)
		if err != nil {
			return errors.Wrap(err, "cache remote config")
		}
	}

	cacheConfig, err := cache.Build(
		cache.With(*cacheType),
		cache.WithSize(*cacheSize),
		cache.WithRemoteConfig(remoteCacheConfig),
	)
	if err != nil {
		return errors.Wrap(err, "cache config")
	}

	supervisor, err := cache.New(cacheConfig, log.With(logger, "component", "cache"))
	if err != nil {
		return errors.Wrap(err, "cache")
	}

	// Execution group.
	g := gexec.NewGroup()
	gexec.Block(g)
	{
		g.Add(func() error {
			mux := http.NewServeMux()
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
	bindAddr, advertiseAddr string,
	peers []string,
) (*cache.RemoteConfig, error) {
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
		members.WithBindAddrPort(clusterBindHost, clusterAdvertisePort),
		members.WithAdvertiseAddrPort(clusterAdvertiseHost, clusterAdvertisePort),
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

	return cache.BuildConfig(
		cache.WithReplicationFactor(replicationFactor),
		cache.WithPeer(cluster.NewPeer(cacheMembers, log.With(logger, "component", "peer"))),
	)
}

type membersLogOutput struct {
	logger log.Logger
}

func (m membersLogOutput) Write(b []byte) (int, error) {
	level.Debug(m.logger).Log("fwd_msg", string(b))
	return len(b), nil
}
