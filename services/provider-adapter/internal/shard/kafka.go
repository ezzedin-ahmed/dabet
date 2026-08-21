package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/kafkax"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Defaults for the Kafka coordinator. The topic carries no records; see
// the package comment.
const (
	DefaultGroup           = "provider-adapter-shards"
	DefaultTopic           = "adapter.shards.v1"
	DefaultSessionTimeout  = 30 * time.Second
	DefaultPingInterval    = 5 * time.Second
	protocolName           = "dabet-shard-ring"
	defaultRebalanceWindow = 30 * time.Second

	// DependencyName is the dependency_up label for the coordinator
	// (§4.5). It is the alert that says "the fleet is running on a frozen
	// assignment".
	DependencyName = "shard_coordinator"
)

// KafkaConfig configures a KafkaCoordinator.
type KafkaConfig struct {
	Brokers []string
	// Group is the consumer group whose membership is the adapter fleet.
	// It must differ from every data-path group.
	Group string
	// Topic is the coordination topic. It is subscribed to and never
	// produced to; a consumer group needs something to subscribe to.
	Topic string
	// Self is this instance's stable ID. It must survive a restart of the
	// same logical instance, because it is the ring node name: a fresh ID
	// every restart means every restart reshuffles a segment.
	Self string
	// SessionTimeout is how long the broker waits before declaring a
	// silent member dead. It is the failover latency for a crashed
	// instance's connections and the tolerance for a network blip that
	// would otherwise cause a pointless reconnect storm.
	SessionTimeout time.Duration
	// RebalanceTimeout is how long the broker waits for members to rejoin.
	RebalanceTimeout time.Duration
	// PingInterval is how often connectivity is probed for dependency_up.
	PingInterval time.Duration
	// Up is dependency_up{dependency="shard_coordinator"}.
	Up  prometheus.Gauge
	Log *slog.Logger
}

func (c *KafkaConfig) applyDefaults() {
	if c.Group == "" {
		c.Group = DefaultGroup
	}
	if c.Topic == "" {
		c.Topic = DefaultTopic
	}
	if c.SessionTimeout <= 0 {
		c.SessionTimeout = DefaultSessionTimeout
	}
	if c.RebalanceTimeout <= 0 {
		c.RebalanceTimeout = defaultRebalanceWindow
	}
	if c.PingInterval <= 0 {
		c.PingInterval = DefaultPingInterval
	}
	if c.Log == nil {
		c.Log = slog.New(slog.DiscardHandler)
	}
}

// KafkaCoordinator derives fleet membership from a Kafka consumer group.
//
// It joins cfg.Group on cfg.Topic with a custom balancer (ringBalancer)
// that turns the group into a membership service: members advertise their
// instance IDs, the per-generation leader broadcasts the sorted list back
// to everyone, and Kafka's session timeouts handle liveness. See the
// package comment for why this beats etcd here.
type KafkaCoordinator struct {
	cfg KafkaConfig
	cl  *kgo.Client
	bal *ringBalancer

	changes chan struct{}

	mu        sync.RWMutex
	members   []string
	epoch     uint64
	connected bool
}

var _ Coordinator = (*KafkaCoordinator)(nil)

// NewKafkaCoordinator connects the coordination client. It does not block
// on the group forming; call Run.
func NewKafkaCoordinator(cfg KafkaConfig) (*KafkaCoordinator, error) {
	cfg.applyDefaults()
	if cfg.Self == "" {
		return nil, errors.New("shard: instance ID is required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("shard: no kafka brokers configured")
	}

	c := &KafkaCoordinator{cfg: cfg, changes: make(chan struct{}, 1)}
	c.bal = &ringBalancer{self: cfg.Self, onView: c.setMembers}

	// The coordinator is a direct kgo client, so it must pick up the shared
	// KAFKA_TLS_*/KAFKA_SASL_* settings itself; otherwise the ring silently
	// stays plaintext against a managed broker while the adapter's kafkax
	// clients authenticate normally.
	sec, err := kafkax.DefaultSecurityConfig()
	if err != nil {
		return nil, fmt.Errorf("shard: kafka security: %w", err)
	}
	secOpts, err := sec.Options()
	if err != nil {
		return nil, fmt.Errorf("shard: kafka security: %w", err)
	}

	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.Balancers(c.bal),
		// The topic is a membership handle, not a log: never commit, and
		// start at the end so a restart never replays anything that
		// somehow got written to it.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.RebalanceTimeout(cfg.RebalanceTimeout),
		kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) {
			c.setConnected(false, "group lost")
		}),
	}, secOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("shard coordinator: %w", err)
	}
	c.cl = cl
	return c, nil
}

// Run maintains the membership view until ctx is cancelled.
//
// Per P2 it never returns on a coordinator failure: an unreachable broker
// flips dependency_up to 0 and the last known membership keeps being
// served, so every watch loop stays up. franz-go reconnects and rejoins on
// its own; there is nothing to restart.
func (c *KafkaCoordinator) Run(ctx context.Context) error {
	c.ensureTopic(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.drain(ctx)
	}()

	tick := time.NewTicker(c.cfg.PingInterval)
	defer tick.Stop()
	c.probe(ctx)
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-tick.C:
			c.probe(ctx)
		}
	}
}

// drain polls and discards. Nothing produces to the coordination topic, so
// this normally blocks forever; it exists because a group consumer that is
// never polled would let fetch buffers accumulate if anything ever did
// write to the topic, and because it is the natural place to observe
// broker errors.
func (c *KafkaCoordinator) drain(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) {
				return
			}
			c.setConnected(false, fe.Err.Error())
		}
	}
}

// probe drives dependency_up. A ping is the only honest liveness signal
// for a member holding no partitions: rebalance callbacks fire on the
// member that owns the coordination topic's partition, and in a fleet of
// N that is exactly one instance.
func (c *KafkaCoordinator) probe(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, c.cfg.PingInterval)
	defer cancel()
	if err := c.cl.Ping(pctx); err != nil {
		if ctx.Err() == nil {
			c.setConnected(false, err.Error())
		}
		return
	}
	c.setConnected(true, "")
}

// ensureTopic creates the coordination topic if it is missing, best
// effort. Broker defaults are requested for partitions and replication
// (-1/-1) because the topic never holds a record and its layout is
// irrelevant. A failure here is not fatal: auto-creation or an operator
// may already have handled it, and the group will form as soon as the
// topic exists.
func (c *KafkaCoordinator) ensureTopic(ctx context.Context) {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10_000
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic = c.cfg.Topic
	t.NumPartitions = -1
	t.ReplicationFactor = -1
	req.Topics = append(req.Topics, t)

	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := req.RequestWith(cctx, c.cl)
	if err != nil {
		c.cfg.Log.Warn("could not create shard coordination topic; relying on auto-creation",
			"topic", c.cfg.Topic, "error", err.Error())
		return
	}
	for _, rt := range resp.Topics {
		if kerr.ErrorForCode(rt.ErrorCode) == nil || rt.ErrorCode == kerr.TopicAlreadyExists.Code {
			continue
		}
		c.cfg.Log.Warn("could not create shard coordination topic; relying on auto-creation",
			"topic", rt.Topic, "error", kerr.ErrorForCode(rt.ErrorCode).Error())
	}
}

// Membership implements Coordinator.
func (c *KafkaCoordinator) Membership() Membership {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Membership{
		Self:      c.cfg.Self,
		Members:   slices.Clone(c.members),
		Epoch:     c.epoch,
		Connected: c.connected,
	}
}

// Changes implements Coordinator.
func (c *KafkaCoordinator) Changes() <-chan struct{} { return c.changes }

// Close leaves the group and closes the client. Leaving promptly is what
// makes a rolling deploy cheap: the group rebalances on the LeaveGroup
// rather than waiting out the session timeout, so the departing
// instance's segment is picked up in milliseconds instead of tens of
// seconds.
func (c *KafkaCoordinator) Close() {
	if c.cl != nil {
		c.cl.Close()
	}
}

// setMembers records a new membership view from the balancer.
//
// The view is never cleared, only replaced: an empty view, or one that
// does not contain this instance, is rejected rather than applied. Both
// could only shrink the ring and tear down watch loops, and neither can
// be true of a generation this instance participated in — the leader
// balances exactly the members that joined, and this instance is one of
// them. Rejecting them is cheap insurance against a bad generation
// costing every stream on this instance. See the package comment's
// failure stance.
func (c *KafkaCoordinator) setMembers(members []string) {
	if len(members) == 0 || !slices.Contains(members, c.cfg.Self) {
		if len(members) > 0 {
			c.cfg.Log.Warn("ignoring a shard membership view that excludes this instance",
				"self", c.cfg.Self, "members", len(members))
		}
		return
	}
	c.mu.Lock()
	changed := !slices.Equal(members, c.members)
	if changed {
		c.members = slices.Clone(members)
		c.epoch++
	}
	c.connected = true
	epoch := c.epoch
	c.mu.Unlock()

	if c.cfg.Up != nil {
		c.cfg.Up.Set(1)
	}
	if !changed {
		return
	}
	c.cfg.Log.Info("shard coordinator synced", "self", c.cfg.Self,
		"members", len(members), "epoch", epoch)
	select {
	case c.changes <- struct{}{}:
	default:
	}
}

func (c *KafkaCoordinator) setConnected(up bool, reason string) {
	c.mu.Lock()
	was := c.connected
	c.connected = up
	held := len(c.members)
	c.mu.Unlock()

	if c.cfg.Up != nil {
		if up {
			c.cfg.Up.Set(1)
		} else {
			c.cfg.Up.Set(0)
		}
	}
	if was == up {
		return
	}
	if up {
		c.cfg.Log.Info("shard coordinator reachable again", "self", c.cfg.Self)
		return
	}
	// P2/N2: this is a warning, not a shutdown. Every watch loop keeps
	// running on the held assignment.
	c.cfg.Log.Warn("shard coordinator unreachable; holding last assignment",
		"self", c.cfg.Self, "members", held, "reason", reason)
}

// ---------------------------------------------------------------------
// ringBalancer — the consumer group as a membership service
// ---------------------------------------------------------------------

// joinData is what a member advertises about itself in JoinGroup.
type joinData struct {
	InstanceID string `json:"instance_id"`
}

// syncData is what the generation's leader broadcasts to every member in
// its sync assignment. This is the only channel Kafka gives a non-leader
// to learn who else is in the group.
type syncData struct {
	Members []string `json:"members"`
}

// ringBalancer implements kgo.GroupBalancer. Partition assignment is
// incidental — the coordination topic has no data — so it hands every
// partition to the first member and spends its effort on the member list.
type ringBalancer struct {
	self   string
	onView func(members []string)
}

var (
	_ kgo.GroupBalancer           = (*ringBalancer)(nil)
	_ kgo.ConsumerBalancerBalance = (*ringBalancer)(nil)
)

// ProtocolName implements kgo.GroupBalancer. It is distinct from the
// built-in names so a stray range/sticky consumer cannot join this group
// and be handed a member list it would not understand.
func (b *ringBalancer) ProtocolName() string { return protocolName }

// IsCooperative implements kgo.GroupBalancer. Eager: there is nothing to
// hand over incrementally, because the partitions are meaningless.
func (b *ringBalancer) IsCooperative() bool { return false }

// JoinGroupMetadata implements kgo.GroupBalancer, advertising this
// instance's stable ID in the member metadata's user data.
func (b *ringBalancer) JoinGroupMetadata(topics []string, _ map[string][]int32, generation int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 1
	meta.Topics = topics
	meta.Generation = generation
	meta.UserData, _ = json.Marshal(joinData{InstanceID: b.self})
	return meta.AppendTo(nil)
}

// ParseSyncAssignment implements kgo.GroupBalancer. Every member — leader
// included — lands here once per generation, which is where the view is
// published to the coordinator.
func (b *ringBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	var ka kmsg.ConsumerMemberAssignment
	if err := ka.ReadFrom(assignment); err != nil {
		return nil, fmt.Errorf("shard: sync assignment parse: %w", err)
	}
	if len(ka.UserData) > 0 && b.onView != nil {
		var sd syncData
		if err := json.Unmarshal(ka.UserData, &sd); err != nil {
			return nil, fmt.Errorf("shard: sync assignment member list: %w", err)
		}
		members := slices.Clone(sd.Members)
		slices.Sort(members)
		members = slices.Compact(members)
		b.onView(members)
	}
	out := make(map[string][]int32, len(ka.Topics))
	for _, t := range ka.Topics {
		out[t.Topic] = t.Partitions
	}
	return out, nil
}

// MemberBalancer implements kgo.GroupBalancer.
func (b *ringBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	cb, err := kgo.NewConsumerBalancer(b, members)
	if cb == nil {
		return nil, nil, err
	}
	return cb, cb.MemberTopics(), err
}

// Balance runs on the generation's leader only. It collects every
// member's advertised instance ID and returns an assignment that gives
// each member the same sorted list, so the whole fleet leaves SyncGroup
// with an identical ring input.
//
// A member whose user data is missing or unparseable falls back to its
// Kafka member ID. That is not a stable ring node name, so such a member
// will reshuffle a segment when it reconnects — but it is a live process
// holding connections and leaving it out of the ring would mean two
// instances believing they own its segment, which is worse.
func (b *ringBalancer) Balance(cb *kgo.ConsumerBalancer, topics map[string]int32) kgo.IntoSyncAssignment {
	ids := make([]string, 0, len(cb.Members()))
	cb.EachMember(func(m *kmsg.JoinGroupResponseMember, meta *kmsg.ConsumerMemberMetadata) {
		id := ""
		if len(meta.UserData) > 0 {
			var jd joinData
			if err := json.Unmarshal(meta.UserData, &jd); err == nil {
				id = jd.InstanceID
			}
		}
		if id == "" {
			id = m.MemberID
		}
		ids = append(ids, id)
	})
	slices.Sort(ids)
	ids = slices.Compact(ids)

	userData, err := json.Marshal(syncData{Members: ids})
	if err != nil {
		cb.SetError(fmt.Errorf("shard: encode member list: %w", err))
		return nil
	}

	// Partitions are irrelevant to this group's purpose; give them all to
	// the lowest member ID so the plan is deterministic and every other
	// member's assignment is user data only.
	plan := make([]kmsg.SyncGroupRequestGroupAssignment, 0, len(cb.Members()))
	for i := range cb.Members() {
		m, meta := cb.MemberAt(i)
		var ka kmsg.ConsumerMemberAssignment
		ka.UserData = userData
		if i == 0 {
			for _, topic := range meta.Topics {
				n, ok := topics[topic]
				if !ok || n <= 0 {
					continue
				}
				at := kmsg.NewConsumerMemberAssignmentTopic()
				at.Topic = topic
				for p := int32(0); p < n; p++ {
					at.Partitions = append(at.Partitions, p)
				}
				ka.Topics = append(ka.Topics, at)
			}
		}
		sa := kmsg.NewSyncGroupRequestGroupAssignment()
		sa.MemberID = m.MemberID
		sa.MemberAssignment = ka.AppendTo(nil)
		plan = append(plan, sa)
	}
	return syncPlan(plan)
}

type syncPlan []kmsg.SyncGroupRequestGroupAssignment

// IntoSyncAssignment implements kgo.IntoSyncAssignment.
func (p syncPlan) IntoSyncAssignment() []kmsg.SyncGroupRequestGroupAssignment { return p }
