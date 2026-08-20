#!/usr/bin/env bash
# Render the chart across a matrix of enable/disable combinations and push
# every result through `kubectl apply --dry-run=client`.
#
# The combinatorial check is the point of the toggles: this chart's whole
# reason for having an `enabled` flag on every component is that a
# deployment can mix self-hosted pieces with AWS managed ones, and the only
# way to know that all of those combinations render is to render them.
#
#   ./hack/render-matrix.sh            # render + client-side dry run
#   NO_KUBECTL=1 ./hack/render-matrix.sh   # render only (no cluster/kubeconfig)
set -uo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-$(mktemp -d)}"
mkdir -p "${OUT}"
FAILED=0
PASSED=0

# `kubectl apply --dry-run=client` still fetches the OpenAPI schema from the
# API server, so it needs a reachable cluster despite the name. Probe once and
# fall back to render-only, so this runs in CI and on a laptop with no cluster.
KUBECTL_OK=""
if [ -z "${NO_KUBECTL:-}" ]; then
  if kubectl ${KUBE_CONTEXT:+--context "${KUBE_CONTEXT}"} version >/dev/null 2>&1; then
    KUBECTL_OK=1
  else
    echo "note: no reachable cluster - rendering only, skipping kubectl dry-run"
  fi
fi

run() {
  local label="$1"; shift
  local file="${OUT}/$(echo "${label}" | tr ' /=,' '____').yaml"

  if ! helm template deps "${CHART}" "$@" > "${file}" 2>"${file}.err"; then
    echo "RENDER FAIL  ${label}"
    sed 's/^/    /' "${file}.err" | head -5
    FAILED=$((FAILED + 1))
    return
  fi

  if [ -z "${NO_KUBECTL:-}" ] && [ -n "${KUBECTL_OK:-}" ]; then
    if ! kubectl ${KUBE_CONTEXT:+--context "${KUBE_CONTEXT}"} apply --dry-run=client -f "${file}" >/dev/null 2>"${file}.kerr"; then
      echo "DRYRUN FAIL  ${label}"
      sed 's/^/    /' "${file}.kerr" | head -10
      FAILED=$((FAILED + 1))
      return
    fi
  fi

  local docs
  docs=$(grep -c '^kind:' "${file}" || true)
  printf 'ok  %-58s %3s objects\n' "${label}" "${docs}"
  PASSED=$((PASSED + 1))
}

echo "rendering into ${OUT}"
echo

# ---- the two shipped profiles -------------------------------------------
run "values.yaml (defaults)"
run "values-local.yaml"  -f "${CHART}/values-local.yaml"
run "values-prod.yaml"   -f "${CHART}/values-prod.yaml"

# ---- everything on, including the opt-in components ----------------------
run "everything on (milvus distributed + gpu fleet)" \
  --set milvus.enabled=true \
  --set vllm.enabled=true --set mocks.llm.enabled=false \
  --set embedding.enabled=true --set mocks.embedding.enabled=false

run "everything on, milvus standalone" \
  --set milvus.enabled=true --set milvus.mode=standalone

run "clickhouse replicated (2x2 + keeper)" \
  --set clickhouse.replicas=4 --set clickhouse.shards=2 \
  --set clickhouse.replicasPerShard=2 --set clickhouse.keeper.enabled=true

# ---- one component off at a time ----------------------------------------
for c in kafka postgres redis memcached clickhouse minio; do
  run "only ${c} disabled" --set "${c}.enabled=false"
done

run "kafka topics job disabled"  --set kafka.topics.enabled=false
run "redis standalone"           --set redis.cluster.enabled=false
run "redis cluster init off"     --set redis.init.enabled=false
run "no mocks at all"            --set mocks.llm.enabled=false --set mocks.embedding.enabled=false
run "connection configmap off"   --set connectionSecret.configMap.enabled=false
run "one postgres instance only" --set postgres.instances.policy.enabled=false
run "no persistence anywhere" \
  --set kafka.persistence.enabled=false \
  --set redis.persistence.enabled=false \
  --set clickhouse.persistence.enabled=false \
  --set minio.persistence.enabled=false \
  --set postgres.defaults.persistence.enabled=false

# ---- THE AWS PATH: managed services standing in for each component ------
# This is the combination Terraform produces. Nothing self-hosted survives
# except the pieces AWS has no equivalent for.
run "full AWS: MSK + RDS + ElastiCache + ElastiCache-memcached + S3" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="b-1.msk.eu-west-1.amazonaws.com:9092\,b-2.msk.eu-west-1.amazonaws.com:9092" \
  --set postgres.enabled=false \
  --set external.postgres.identity="postgres://dabet:pw@identity.rds.amazonaws.com:5432/dabet_identity?sslmode=require" \
  --set external.postgres.policy="postgres://dabet:pw@policy.rds.amazonaws.com:5432/dabet_policy?sslmode=require" \
  --set redis.enabled=false \
  --set external.redis.addr="dabet.abc.clustercfg.euw1.cache.amazonaws.com:6379" \
  --set external.redis.cluster=true \
  --set memcached.enabled=false \
  --set external.memcached.addrs="dabet.abc.cfg.euw1.cache.amazonaws.com:11211" \
  --set minio.enabled=false \
  --set external.s3.bucket=dabet-embeddings-prod \
  --set external.s3.region=eu-west-1

run "AWS partial: MSK + RDS, self-hosted redis/memcached/clickhouse/minio" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="b-1.msk:9092" \
  --set postgres.enabled=false \
  --set external.postgres.identity="postgres://u:p@a:5432/i" \
  --set external.postgres.policy="postgres://u:p@b:5432/p"

# ---- the empty release: every single component off ----------------------
# Must still render, and must publish a Secret with only the keys that were
# externally configured. This is the degenerate case that proves the
# contract is not coupled to any component.
run "EVERYTHING disabled" \
  --set kafka.enabled=false --set postgres.enabled=false \
  --set redis.enabled=false --set memcached.enabled=false \
  --set clickhouse.enabled=false --set minio.enabled=false \
  --set milvus.enabled=false --set vllm.enabled=false \
  --set embedding.enabled=false \
  --set mocks.llm.enabled=false --set mocks.embedding.enabled=false

echo
echo "passed=${PASSED} failed=${FAILED}"
[ "${FAILED}" -eq 0 ]
