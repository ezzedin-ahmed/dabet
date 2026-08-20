package shard

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// joinAs builds the JoinGroup member record a balancer would produce for
// an instance, so the balancer can be driven with no broker involved.
func joinAs(instanceID, memberID string, topics []string) kmsg.JoinGroupResponseMember {
	b := &ringBalancer{self: instanceID}
	m := kmsg.NewJoinGroupResponseMember()
	m.MemberID = memberID
	m.ProtocolMetadata = b.JoinGroupMetadata(topics, nil, 3)
	return m
}

// balanceOnce runs one generation of the group protocol in memory: the
// leader balances, and every member parses the assignment addressed to it.
// It returns each member ID's resulting membership view.
func balanceOnce(t *testing.T, members []kmsg.JoinGroupResponseMember, topics map[string]int32) map[string][]string {
	t.Helper()

	leader := &ringBalancer{self: "leader-does-not-matter"}
	gb, _, err := leader.MemberBalancer(members)
	if err != nil {
		t.Fatalf("MemberBalancer: %v", err)
	}
	orErr, ok := gb.(kgo.GroupMemberBalancerOrError)
	if !ok {
		t.Fatalf("balancer %T does not report errors", gb)
	}
	into, err := orErr.BalanceOrError(topics)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	assignments := into.IntoSyncAssignment()
	if len(assignments) != len(members) {
		t.Fatalf("balanced %d members into %d assignments", len(members), len(assignments))
	}

	views := make(map[string][]string, len(members))
	for _, sa := range assignments {
		var view []string
		b := &ringBalancer{self: sa.MemberID, onView: func(m []string) { view = m }}
		if _, err := b.ParseSyncAssignment(sa.MemberAssignment); err != nil {
			t.Fatalf("ParseSyncAssignment for %q: %v", sa.MemberID, err)
		}
		views[sa.MemberID] = view
	}
	return views
}

// TestBalancerBroadcastsTheSameMembershipToEveryMember is the whole point
// of the custom balancer: Kafka tells a member only its own assignment, so
// the leader must echo the member list back or non-leaders could never
// build the ring. Every member must end the generation with the identical
// list, because a member with a different view computes a different
// assignment and either double-owns or orphans connections.
func TestBalancerBroadcastsTheSameMembershipToEveryMember(t *testing.T) {
	topics := map[string]int32{DefaultTopic: 3}
	members := []kmsg.JoinGroupResponseMember{
		joinAs("adapter-c", "kafka-member-1", []string{DefaultTopic}),
		joinAs("adapter-a", "kafka-member-2", []string{DefaultTopic}),
		joinAs("adapter-b", "kafka-member-3", []string{DefaultTopic}),
	}

	views := balanceOnce(t, members, topics)
	want := []string{"adapter-a", "adapter-b", "adapter-c"}
	for memberID, got := range views {
		if !slices.Equal(got, want) {
			t.Errorf("member %q learned membership %v, want %v", memberID, got, want)
		}
	}

	// The list is sorted independently of join order, so the ring is
	// identical whichever member happened to be leader.
	slices.Reverse(members)
	for memberID, got := range balanceOnce(t, members, topics) {
		if !slices.Equal(got, want) {
			t.Errorf("with members joining in the opposite order, %q learned %v, want %v", memberID, got, want)
		}
	}
}

// TestBalancerFallsBackToKafkaMemberID: a member that did not advertise a
// usable instance ID must still appear in the ring. Leaving it out would
// mean the rest of the fleet believes it owns connections that process is
// still watching.
func TestBalancerFallsBackToKafkaMemberID(t *testing.T) {
	silent := kmsg.NewJoinGroupResponseMember()
	silent.MemberID = "kafka-member-mystery"
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 1
	meta.Topics = []string{DefaultTopic}
	silent.ProtocolMetadata = meta.AppendTo(nil) // no user data at all

	members := []kmsg.JoinGroupResponseMember{
		joinAs("adapter-a", "kafka-member-1", []string{DefaultTopic}),
		silent,
	}
	for memberID, got := range balanceOnce(t, members, map[string]int32{DefaultTopic: 1}) {
		want := []string{"adapter-a", "kafka-member-mystery"}
		if !slices.Equal(got, want) {
			t.Errorf("member %q learned %v, want %v", memberID, got, want)
		}
	}
}

// TestBalancerAssignsEveryPartitionExactlyOnce: partitions are incidental
// to this group, but franz-go still consumes what it is given, so the plan
// must be a valid one.
func TestBalancerAssignsEveryPartitionExactlyOnce(t *testing.T) {
	members := []kmsg.JoinGroupResponseMember{
		joinAs("adapter-a", "kafka-member-1", []string{DefaultTopic}),
		joinAs("adapter-b", "kafka-member-2", []string{DefaultTopic}),
	}
	leader := &ringBalancer{self: "adapter-a"}
	gb, _, err := leader.MemberBalancer(members)
	if err != nil {
		t.Fatal(err)
	}
	into, err := gb.(kgo.GroupMemberBalancerOrError).BalanceOrError(map[string]int32{DefaultTopic: 4})
	if err != nil {
		t.Fatal(err)
	}

	seen := map[int32]string{}
	for _, sa := range into.IntoSyncAssignment() {
		b := &ringBalancer{self: sa.MemberID}
		got, err := b.ParseSyncAssignment(sa.MemberAssignment)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range got[DefaultTopic] {
			if other, dup := seen[p]; dup {
				t.Fatalf("partition %d assigned to both %q and %q", p, other, sa.MemberID)
			}
			seen[p] = sa.MemberID
		}
	}
	if len(seen) != 4 {
		t.Errorf("assigned %d of 4 partitions", len(seen))
	}
}

// TestKafkaCoordinatorHoldsMembershipView pins the P2 behaviour of the
// real coordinator without a broker: once a view exists, neither a
// disconnect nor an empty sync may clear it.
func TestKafkaCoordinatorHoldsMembershipView(t *testing.T) {
	up := prometheus.NewGauge(prometheus.GaugeOpts{Name: "dependency_up_test"})
	c := &KafkaCoordinator{
		cfg:     KafkaConfig{Self: "adapter-a", Up: up},
		changes: make(chan struct{}, 1),
	}
	c.cfg.applyDefaults()

	c.setMembers([]string{"adapter-a", "adapter-b"})
	if got := c.Membership(); !slices.Equal(got.Members, []string{"adapter-a", "adapter-b"}) || !got.Connected || got.Epoch != 1 {
		t.Fatalf("after the first sync: %+v", got)
	}
	awaitSignal(t, c.Changes(), "the first sync")

	// A repeat of the same view is not a rebalance and must not signal.
	c.setMembers([]string{"adapter-a", "adapter-b"})
	if got := c.Membership(); got.Epoch != 1 {
		t.Errorf("an unchanged membership bumped the epoch to %d", got.Epoch)
	}
	select {
	case <-c.Changes():
		t.Error("an unchanged membership signalled a change")
	default:
	}

	// An empty view is rejected outright: it could only ever shrink the
	// ring and tear down watch loops.
	c.setMembers(nil)
	if got := c.Membership(); len(got.Members) != 2 {
		t.Errorf("an empty sync cleared the membership view: %+v", got)
	}

	// So is one that leaves this instance out: it cannot be true of a
	// generation this instance joined.
	c.setMembers([]string{"adapter-b", "adapter-c"})
	if got := c.Membership(); !slices.Equal(got.Members, []string{"adapter-a", "adapter-b"}) {
		t.Errorf("a view excluding this instance was applied: %+v", got)
	}

	// Losing the broker holds the view and drops dependency_up.
	c.setConnected(false, "test")
	got := c.Membership()
	if !slices.Equal(got.Members, []string{"adapter-a", "adapter-b"}) {
		t.Errorf("a disconnect changed the membership view: %+v", got)
	}
	if got.Connected {
		t.Error("still reporting connected after a disconnect")
	}

	c.setConnected(true, "")
	if !c.Membership().Connected {
		t.Error("did not report reconnection")
	}
}

// TestKafkaCoordinatorRejectsBadConfig: an instance with no stable ID
// would be a new ring node on every restart, reshuffling a segment each
// time. Fail at startup instead.
func TestKafkaCoordinatorRejectsBadConfig(t *testing.T) {
	if _, err := NewKafkaCoordinator(KafkaConfig{Brokers: []string{"localhost:9092"}}); err == nil {
		t.Error("accepted an empty instance ID")
	}
	if _, err := NewKafkaCoordinator(KafkaConfig{Self: "adapter-a"}); err == nil {
		t.Error("accepted an empty broker list")
	}
}
