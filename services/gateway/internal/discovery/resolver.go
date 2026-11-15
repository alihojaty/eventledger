package discovery

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

const (
	Scheme             = "swarmdns"
	minResolveInterval = 250 * time.Millisecond
)

type Builder struct {
	refreshInterval     time.Duration
	retireGrace         time.Duration
	onEndpointsResolved func(count int)
	logger              *zap.Logger
}

func NewBuilder(refreshInterval time.Duration, logger *zap.Logger, onEndpoints ...func(int)) *Builder {
	if refreshInterval <= 0 {
		refreshInterval = 2 * time.Second
	}
	var cb func(int)
	if len(onEndpoints) > 0 {
		cb = onEndpoints[0]
	}
	return &Builder{
		refreshInterval:     refreshInterval,
		retireGrace:         15 * time.Second,
		onEndpointsResolved: cb,
		logger:              logger,
	}
}

func (b *Builder) Scheme() string {
	return Scheme
}

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint()
	if endpoint == "" {
		endpoint = target.URL.Host
	}
	if endpoint == "" {
		endpoint = strings.TrimPrefix(target.URL.Opaque, "//")
	}

	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		host = endpoint
		port = "50051"
	}

	tlsServerName := strings.TrimPrefix(host, "tasks.")
	if tlsServerName == "" {
		tlsServerName = host
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &swarmDNSResolver{
		host:                host,
		tlsServerName:       tlsServerName,
		port:                port,
		cc:                  cc,
		refreshInterval:     b.refreshInterval,
		retireGrace:         b.retireGrace,
		onEndpointsResolved: b.onEndpointsResolved,
		logger:              b.logger,
		ctx:                 ctx,
		cancel:              cancel,
		resolveNowCh:        make(chan struct{}, 1),
		knownIPs:            make(map[string]time.Time),
	}

	r.wg.Add(1)
	go r.loop()
	return r, nil
}

type swarmDNSResolver struct {
	host                string
	tlsServerName       string
	port                string
	cc                  resolver.ClientConn
	refreshInterval     time.Duration
	retireGrace         time.Duration
	onEndpointsResolved func(int)
	logger              *zap.Logger

	ctx          context.Context
	cancel       context.CancelFunc
	resolveNowCh chan struct{}
	wg           sync.WaitGroup

	mu          sync.Mutex
	knownIPs    map[string]time.Time
	lastResolve time.Time
}

func (r *swarmDNSResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case r.resolveNowCh <- struct{}{}:
	default:
	}
}

func (r *swarmDNSResolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *swarmDNSResolver) loop() {
	defer r.wg.Done()

	r.resolve(true)

	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.resolve(true)
		case <-r.resolveNowCh:
			r.resolve(false)
		}
	}
}

func (r *swarmDNSResolver) lookupCandidateIPs() ([]string, error) {
	lookupCtx, cancel := context.WithTimeout(r.ctx, 1500*time.Millisecond)
	defer cancel()

	if !strings.HasPrefix(r.host, "tasks.") && r.host != "localhost" && net.ParseIP(r.host) == nil {
		if ips, err := net.DefaultResolver.LookupHost(lookupCtx, "tasks."+r.host); err == nil && len(ips) > 0 {
			return ips, nil
		}
	}

	return net.DefaultResolver.LookupHost(lookupCtx, r.host)
}

func (r *swarmDNSResolver) resolve(force bool) {
	r.mu.Lock()
	if !force && time.Since(r.lastResolve) < minResolveInterval {
		r.mu.Unlock()
		return
	}
	r.lastResolve = time.Now()
	r.mu.Unlock()

	now := time.Now()
	ips, err := r.lookupCandidateIPs()

	r.mu.Lock()
	if err == nil && len(ips) > 0 {
		for _, ip := range ips {
			r.knownIPs[ip] = now
		}
		// Only expire retired IPs when DNS resolution succeeded so a temporary
		// DNS hiccup never wipes out healthy active publisher subchannels.
		for ip, lastSeen := range r.knownIPs {
			if now.Sub(lastSeen) > r.retireGrace {
				delete(r.knownIPs, ip)
			}
		}
	} else if err != nil && r.logger != nil {
		r.logger.Debug("Transient DNS lookup failure in swarmdns resolver; retaining last known IPs",
			zap.String("host", r.host),
			zap.Error(err),
		)
	}

	activeIPs := make([]string, 0, len(r.knownIPs))
	for ip := range r.knownIPs {
		activeIPs = append(activeIPs, ip)
	}
	r.mu.Unlock()

	sort.Strings(activeIPs)

	var addrs []resolver.Address
	if len(activeIPs) > 0 {
		addrs = make([]resolver.Address, 0, len(activeIPs))
		for _, ip := range activeIPs {
			addrs = append(addrs, resolver.Address{
				Addr:       net.JoinHostPort(ip, r.port),
				ServerName: r.tlsServerName,
			})
		}
	} else {
		addrs = []resolver.Address{{
			Addr:       net.JoinHostPort(r.host, r.port),
			ServerName: r.tlsServerName,
		}}
	}

	if r.onEndpointsResolved != nil {
		r.onEndpointsResolved(len(addrs))
	}

	_ = r.cc.UpdateState(resolver.State{Addresses: addrs})
}
