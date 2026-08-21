#!/bin/bash
# Load campaign: one rung per rate, clean state between rungs, per-component
# CPU sampled during each rung, generator utilisation recorded alongside.
export KUBECONFIG=/tmp/k3s-dabet.yaml
NODE=192.168.77.2
export KAFKA_BROKERS=$NODE:31090,$NODE:31091,$NODE:31092
export LOAD_MODERATION_METRICS=http://$NODE:31099
export LOAD_USER_URL=http://$NODE:31081 LOAD_CREDITS_URL=http://$NODE:31082
export LOAD_POLICY_URL=http://$NODE:31083 LOAD_ADAPTER_URL=http://$NODE:31084
export LOAD_REVIEW_URL=http://$NODE:31086
OUT=${OUT:-/tmp/campaign}; mkdir -p $OUT
LABEL=${LABEL:-run}

reset_state() {
  # A true clean baseline. drain() only waits for the consumer to catch up; it
  # leaves the retained log in place, and by the end of a ladder that was GBs
  # of accumulated segments quietly changing what the brokers had to do.
  # Partition counts and retentions mirror the chart's local profile.
  kubectl exec -n dabet dabet-deps-kafka-0 -- bash -c '
    K=/opt/kafka/bin/kafka-topics.sh
    for t in messages.v1 flagged.v1 deletions.v1 usage.v1; do
      $K --bootstrap-server localhost:9092 --delete --topic $t 2>/dev/null
    done
    sleep 15
    $K --bootstrap-server localhost:9092 --create --topic messages.v1  --partitions 12 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000 2>/dev/null
    $K --bootstrap-server localhost:9092 --create --topic flagged.v1   --partitions 6  --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000 2>/dev/null
    $K --bootstrap-server localhost:9092 --create --topic deletions.v1 --partitions 6  --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000 2>/dev/null
    $K --bootstrap-server localhost:9092 --create --topic usage.v1     --partitions 3  --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000 2>/dev/null
  ' >/dev/null 2>&1
  # Consumers hold metadata for topics that no longer exist; restart so each
  # rung starts from a fresh group generation as well as a fresh log.
  kubectl -n dabet rollout restart deploy/dabet-moderation-service >/dev/null 2>&1
  kubectl -n dabet rollout status deploy/dabet-moderation-service --timeout=180s >/dev/null 2>&1
  sleep 15
}

drain() {  # wait for the consumer group to catch up before the next rung
  for i in $(seq 1 60); do
    L=$(curl -s --max-time 4 http://$NODE:31099/metrics | awk '/^kafka_consumer_lag_messages/{s+=$2} END{printf "%d", s+0}')
    [ "${L:-1}" -lt 300 ] && return 0
    sleep 15
  done
  echo "WARN: did not drain" >&2
}

sample() {  # peak millicores per component during the rung
  local tag=$1
  for i in $(seq 1 18); do
    kubectl top pods -n dabet --no-headers 2>/dev/null \
      | awk '{gsub("m","",$2); print $1"\t"$2}'
    sleep 5
  done | awk -F'\t' '{if($2>m[$1])m[$1]=$2} END{for(p in m) printf "%s\t%s\n",p,m[p]}' \
      | sort -k2 -rn > $OUT/$tag.cpu
}

for RATE in "$@"; do
  TAG="${LABEL}-${RATE}"
  echo "=== rung: $RATE msg/s  ($LABEL) ==="
  drain
  reset_state
  /tmp/dabet-load -scenario baseline -rate $RATE -out $OUT > $OUT/$TAG.txt 2>&1 &
  RUNPID=$!
  sleep 20
  # Generator utilisation, sampled from its own /proc entry. The skill is
  # explicit: above ~70% of a core-equivalent and the result is the
  # GENERATOR's limit, so it must be reported as a floor, not a ceiling.
  ( for i in $(seq 1 15); do
      a=$(awk '{print $14+$15}' /proc/$RUNPID/stat 2>/dev/null || echo 0)
      sleep 2
      b=$(awk '{print $14+$15}' /proc/$RUNPID/stat 2>/dev/null || echo 0)
      awk -v a="$a" -v b="$b" -v hz="$(getconf CLK_TCK)" 'BEGIN{printf "%.1f\n", (b-a)/2/hz*100}' 
    done | sort -rn | head -1 ) > $OUT/$TAG.genpct
  sample "$TAG"
  wait $RUNPID
  echo "  gen CPU%: $(cat $OUT/$TAG.genpct 2>/dev/null)  (of $(nproc) cores)"
  grep -E "consumed what|end-to-end latency|RESULT:" $OUT/$TAG.txt | sed 's/^/  /'
  head -3 $OUT/$TAG.cpu | sed 's/^/  cpu: /'
done
