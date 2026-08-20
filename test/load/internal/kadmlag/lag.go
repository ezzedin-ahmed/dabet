// Package kadmlag samples Kafka consumer-group lag per partition
// straight from the broker.
//
// It does this rather than reading kafka_consumer_lag_messages off
// /metrics because no Dabet service ever sets that gauge: pkg/obs
// declares it and nothing writes to it, so the family is absent from
// every /metrics response (see the harness README, finding F1). §4.7
// makes growing lag THE overload signal, so the harness measures it
// itself instead of reporting a metric that does not exist.
package kadmlag

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionLag is one partition's backlog for one group.
type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	End       int64  `json:"end_offset"`
	Committed int64  `json:"committed_offset"`
	Lag       int64  `json:"lag"`
}

// Sample is one lag observation across a group's topics.
type Sample struct {
	At         time.Time      `json:"at"`
	Group      string         `json:"group"`
	Total      int64          `json:"total"`
	Max        int64          `json:"max"`
	Partitions []PartitionLag `json:"partitions,omitempty"`
}

// Client samples lag for one consumer group.
type Client struct {
	adm   *kadm.Client
	kcl   *kgo.Client
	group string
}

// New connects an admin client to brokers.
func New(brokers []string, group string) (*Client, error) {
	kcl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka admin client: %w", err)
	}
	return &Client{adm: kadm.NewClient(kcl), kcl: kcl, group: group}, nil
}

// Close releases the connection.
func (c *Client) Close() { c.kcl.Close() }

// Sample reads the group's committed offsets and the topics' end
// offsets and returns the difference per partition. withPartitions
// keeps the per-partition detail, which the hot-spot scenario needs to
// see imbalance and the steady-state scenario does not.
func (c *Client) Sample(ctx context.Context, topics []string, withPartitions bool) (Sample, error) {
	out := Sample{At: time.Now(), Group: c.group}

	ends, err := c.adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return out, fmt.Errorf("list end offsets: %w", err)
	}
	committed, err := c.adm.FetchOffsets(ctx, c.group)
	if err != nil {
		// A group that has never committed is not an error: it is a lag
		// equal to the whole log, which is exactly what we report.
		committed = kadm.OffsetResponses{}
	}

	var parts []PartitionLag
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		var com int64
		if off, ok := committed.Lookup(o.Topic, o.Partition); ok {
			com = off.At
		}
		if com < 0 {
			com = 0
		}
		lag := o.Offset - com
		if lag < 0 {
			lag = 0
		}
		out.Total += lag
		if lag > out.Max {
			out.Max = lag
		}
		parts = append(parts, PartitionLag{
			Topic: o.Topic, Partition: o.Partition,
			End: o.Offset, Committed: com, Lag: lag,
		})
	})
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Topic != parts[j].Topic {
			return parts[i].Topic < parts[j].Topic
		}
		return parts[i].Partition < parts[j].Partition
	})
	if withPartitions {
		out.Partitions = parts
	}
	return out, nil
}

// Partitions reports the partition count of a topic, so a run can state
// plainly whether it measured a 3-way pipe or a realistic one (§4.2
// targets 512 for messages.v1; local compose ships 3).
func (c *Client) Partitions(ctx context.Context, topic string) (int, error) {
	md, err := c.adm.Metadata(ctx, topic)
	if err != nil {
		return 0, err
	}
	t, ok := md.Topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic %s not found", topic)
	}
	return len(t.Partitions), nil
}
