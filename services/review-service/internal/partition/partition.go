// Package partition resolves creator_id -> flagged.v1 partition, matching
// exactly how the producer side places records.
//
// kafkax.NewProducer configures no kgo.RecordPartitioner, so franz-go's
// default applies: UniformBytesPartitioner(64<<10, true, true, nil)
// (kgo/config.go defaultCfg). With keys=true, every record with a non-nil
// key is placed by KafkaHasher(murmur2) — Kafka's Java-default mapping,
// int32(murmur2(key) & 0x7fffffff) % numPartitions — and the uniform-bytes
// batching behaviour applies only to unkeyed records. flagged.v1 is always
// produced with key creator_id (contracts.FlaggedKey), so the keyed path is
// the only one that matters.
//
// Rather than reimplementing murmur2, this package instantiates the same
// default partitioner and drives it through kgo's own interfaces, so the
// mapping is the producer's code path, not a copy of it. A mismatch here
// would silently empty every review queue (§7.6), so the tests cross-check
// the mapping against kgo's StickyKeyPartitioner (an independent kgo path
// through KafkaHasher(murmur2)), against a from-the-Kafka-source murmur2
// reference, and against the partitioner a default-configured kgo.Client
// reports via OptValue.
package partition

import (
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Default returns franz-go's default partitioner, with the exact arguments
// of kgo's defaultCfg. If franz-go ever changes its default, the OptValue
// cross-check test fails loudly rather than the mapping drifting silently.
func Default() kgo.Partitioner {
	return kgo.UniformBytesPartitioner(64<<10, true, true, nil)
}

// Mapper maps record keys to partitions for one topic, exactly as the
// producer's default partitioner would.
type Mapper struct {
	mu sync.Mutex // kgo guarantees single-record use of a TopicPartitioner; HTTP handlers are concurrent
	tp kgo.TopicPartitioner
}

// NewMapper builds a Mapper for topic using the default partitioner.
func NewMapper(topic string) *Mapper {
	return NewMapperWith(Default(), topic)
}

// NewMapperWith builds a Mapper over an explicit partitioner. The tests
// use it to drive the partitioner a default-configured kgo.Client reports,
// proving the mapping matches what kafkax's producer would pick.
func NewMapperWith(p kgo.Partitioner, topic string) *Mapper {
	return &Mapper{tp: p.ForTopic(topic)}
}

// Partition returns the partition the producer would pick for key among
// numPartitions. key must be non-empty (flagged.v1 keys always are): the
// default partitioner places unkeyed records adaptively, not consistently,
// and no stable client-side mapping would exist.
func (m *Mapper) Partition(key []byte, numPartitions int32) (int32, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("partition mapping requires a non-empty key")
	}
	if numPartitions <= 0 {
		return 0, fmt.Errorf("partition mapping requires a positive partition count, got %d", numPartitions)
	}
	rec := &kgo.Record{Key: key}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.tp.RequiresConsistency(rec) {
		// A keyed record not requiring consistency means the partitioner
		// is not key-hashing — the client-side mapping would be garbage.
		return 0, fmt.Errorf("partitioner does not place keyed records consistently")
	}
	if bp, ok := m.tp.(kgo.TopicBackupPartitioner); ok {
		// The uniform-bytes partitioner routes everything through
		// PartitionByBackup; for keyed records the backup iterator is
		// never touched (the hasher decides), but hand it a real one.
		return int32(bp.PartitionByBackup(rec, int(numPartitions), &zeroBackup{rem: int(numPartitions)})), nil
	}
	return int32(m.tp.Partition(rec, int(numPartitions))), nil
}

// zeroBackup is a kgo.TopicBackupIter reporting zero buffered records for
// every partition.
type zeroBackup struct {
	next int
	rem  int
}

func (z *zeroBackup) Next() (int, int64) {
	i := z.next
	z.next++
	z.rem--
	return i, 0
}

func (z *zeroBackup) Rem() int { return z.rem }
