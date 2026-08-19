package partition

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
)

// murmur2 is an independent reference implementation of Kafka's murmur2
// (org.apache.kafka.common.utils.Utils#murmur2), the hash behind the Java
// client's default partitioner. A creator_id must map to the same
// partition here, in kgo's default partitioner, and in
// kgo.StickyKeyPartitioner — otherwise the producer and review-service
// disagree and every queue silently reads empty.
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(data))
	i := 0
	for n := len(data) - (len(data) % 4); i < n; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch len(data) % 4 {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// kafkaDefault mirrors Kafka's DefaultPartitioner arithmetic.
func kafkaDefault(key []byte, n int32) int32 {
	return int32(murmur2(key)&0x7fffffff) % n
}

var sampleCreatorIDs = []string{
	"9d4ec8a1-93b8-4c58-bd21-0c8f8a2f9e11",
	"00000000-0000-0000-0000-000000000000",
	"ffffffff-ffff-ffff-ffff-ffffffffffff",
	"1b7e2ea0-52ad-4f0e-9a3e-6f6f0d5a7c42",
	"c0ffee00-cafe-4bad-8bad-f00dd15ea5e0",
	"7a133700-1111-2222-3333-444455556666",
	"deadbeef-dead-beef-dead-beefdeadbeef",
	"3b71aa12-9c4d-4e21-b1c1-88a08b7c55d9",
}

var partitionCounts = []int32{1, 2, 3, 16, 128, 512}

func TestMapperMatchesKafkaDefaultHashing(t *testing.T) {
	m := NewMapper(contracts.TopicFlagged)
	for _, id := range sampleCreatorIDs {
		key := contracts.FlaggedKey(id)
		for _, n := range partitionCounts {
			got, err := m.Partition(key, n)
			if err != nil {
				t.Fatalf("Partition(%q, %d): %v", id, n, err)
			}
			if want := kafkaDefault(key, n); got != want {
				t.Errorf("creator %s across %d partitions: mapper picked %d, Kafka default murmur2 picks %d", id, n, got, want)
			}
		}
	}
}

// TestMapperMatchesProducerClientPartitioner asks a kgo.Client configured
// exactly like kafkax.NewProducer (no RecordPartitioner override) which
// partitioner it will use, and verifies our mapper places every sample
// key identically. kgo.NewClient does not dial, so no broker is needed.
func TestMapperMatchesProducerClientPartitioner(t *testing.T) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:9092"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	producerPartitioner, ok := cl.OptValue(kgo.RecordPartitioner).(kgo.Partitioner)
	if !ok {
		t.Fatal("client did not report a RecordPartitioner")
	}
	viaProducer := NewMapperWith(producerPartitioner, contracts.TopicFlagged)
	viaMapper := NewMapper(contracts.TopicFlagged)

	for _, id := range sampleCreatorIDs {
		key := contracts.FlaggedKey(id)
		for _, n := range partitionCounts {
			want, err := viaProducer.Partition(key, n)
			if err != nil {
				t.Fatalf("producer partitioner on %q/%d: %v", id, n, err)
			}
			got, err := viaMapper.Partition(key, n)
			if err != nil {
				t.Fatalf("mapper on %q/%d: %v", id, n, err)
			}
			if got != want {
				t.Errorf("creator %s across %d partitions: mapper %d, producer client partitioner %d", id, n, got, want)
			}
		}
	}
}

// TestMapperMatchesStickyKeyPartitioner cross-checks against kgo's
// StickyKeyPartitioner, an independent kgo code path that uses the same
// KafkaHasher(murmur2) for keyed records.
func TestMapperMatchesStickyKeyPartitioner(t *testing.T) {
	sticky := NewMapperWith(kgo.StickyKeyPartitioner(nil), contracts.TopicFlagged)
	m := NewMapper(contracts.TopicFlagged)
	for _, id := range sampleCreatorIDs {
		key := contracts.FlaggedKey(id)
		for _, n := range partitionCounts {
			want, err := sticky.Partition(key, n)
			if err != nil {
				t.Fatalf("sticky on %q/%d: %v", id, n, err)
			}
			got, err := m.Partition(key, n)
			if err != nil {
				t.Fatalf("mapper on %q/%d: %v", id, n, err)
			}
			if got != want {
				t.Errorf("creator %s across %d partitions: mapper %d, sticky-key %d", id, n, got, want)
			}
		}
	}
}

func TestMapperManyKeysInRange(t *testing.T) {
	m := NewMapper(contracts.TopicFlagged)
	const n = int32(128)
	seen := make(map[int32]bool)
	for i := 0; i < 1000; i++ {
		key := contracts.FlaggedKey(fmt.Sprintf("creator-%04d", i))
		p, err := m.Partition(key, n)
		if err != nil {
			t.Fatalf("Partition: %v", err)
		}
		if p < 0 || p >= n {
			t.Fatalf("partition %d out of range [0,%d)", p, n)
		}
		seen[p] = true
	}
	// murmur2 over 1000 keys should touch a healthy share of 128 partitions.
	if len(seen) < 100 {
		t.Errorf("only %d of %d partitions used across 1000 keys; hashing looks broken", len(seen), n)
	}
}

func TestMapperRejectsBadInput(t *testing.T) {
	m := NewMapper(contracts.TopicFlagged)
	if _, err := m.Partition(nil, 16); err == nil {
		t.Error("expected error for empty key")
	}
	if _, err := m.Partition([]byte("x"), 0); err == nil {
		t.Error("expected error for zero partitions")
	}
}
