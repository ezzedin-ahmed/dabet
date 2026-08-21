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

# ---- THE ADMIN JOBS AGAINST AN EXTERNAL BROKER --------------------------
# The gap this chart used to have: the topic reconciler was gated on
# kafka.enabled, so with MSK nothing created the §4.2 topics. These cases
# pin the decoupled behaviour — the Job must render against an external
# bootstrap, with TLS and SASL, and with the credential by reference.
MSK_BOOTSTRAP="b-1.dabet.abc.c2.kafka.eu-west-1.amazonaws.com:9096\,b-2.dabet.abc.c2.kafka.eu-west-1.amazonaws.com:9096"

run "external broker, topic reconciler over TLS + SASL/SCRAM" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="${MSK_BOOTSTRAP}" \
  --set kafka.admin.auth.tls.enabled=true \
  --set kafka.admin.auth.sasl.mechanism=SCRAM-SHA-512 \
  --set kafka.admin.auth.sasl.existingSecret.name=dabet-kafka-admin

run "external broker, private CA bundle for the reconciler" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="b-1.kafka.internal:9093" \
  --set kafka.admin.auth.tls.enabled=true \
  --set kafka.admin.auth.tls.caSecret.name=kafka-ca \
  --set kafka.admin.auth.sasl.mechanism=SCRAM-SHA-512 \
  --set kafka.admin.auth.sasl.existingSecret.name=dabet-kafka-admin

run "external broker + ACL reconciler" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="${MSK_BOOTSTRAP}" \
  --set kafka.admin.auth.tls.enabled=true \
  --set kafka.admin.auth.sasl.mechanism=SCRAM-SHA-512 \
  --set kafka.admin.auth.sasl.existingSecret.name=dabet-kafka-admin \
  --set kafka.acls.enabled=true \
  --set kafka.acls.rules[0].principal="User:dabet-admin" \
  --set kafka.acls.rules[0].resourceType=cluster \
  --set kafka.acls.rules[0].operations="{Describe,Alter,DescribeConfigs,AlterConfigs}" \
  --set kafka.acls.rules[1].principal="User:dabet-moderation-service" \
  --set kafka.acls.rules[1].resourceType=topic \
  --set kafka.acls.rules[1].resourceName=messages.v1 \
  --set kafka.acls.rules[1].operations="{Read,Describe}"

# ACLs on, no rules: nothing to apply, so no Job. Guards against an empty
# ACL Job that would "succeed" while granting nothing.
run "acls enabled but empty rule list" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="${MSK_BOOTSTRAP}" \
  --set kafka.acls.enabled=true

# ---- THE AWS PATH: managed services standing in for each component ------
# This is the combination Terraform produces. Nothing self-hosted survives
# except the pieces AWS has no equivalent for.
run "full AWS: MSK + RDS + ElastiCache + ElastiCache-memcached + S3" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="${MSK_BOOTSTRAP}" \
  --set kafka.admin.auth.tls.enabled=true \
  --set kafka.admin.auth.sasl.mechanism=SCRAM-SHA-512 \
  --set kafka.admin.auth.sasl.existingSecret.name=dabet-kafka-admin \
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

# ---- invariant: the admin credential is NEVER a literal -----------------
# The reconcilers authenticate with a SASL password. There is no values key
# that holds one and there must never be: it reaches the pod as a
# secretKeyRef and becomes a client.properties in a memory-backed emptyDir
# at runtime. This asserts the rendered manifest agrees.
echo
echo "asserting the reconciler credential is by reference only"
cred_out="${OUT}/credential-assert.yaml"
helm template deps "${CHART}" \
  --set kafka.enabled=false \
  --set external.kafka.brokers="${MSK_BOOTSTRAP}" \
  --set kafka.admin.auth.tls.enabled=true \
  --set kafka.admin.auth.sasl.mechanism=SCRAM-SHA-512 \
  --set kafka.admin.auth.sasl.existingSecret.name=dabet-kafka-admin \
  --set kafka.acls.enabled=true \
  --set kafka.acls.rules[0].principal="User:dabet-admin" \
  --set kafka.acls.rules[0].resourceType=cluster \
  --set kafka.acls.rules[0].operations="{Describe,Alter}" \
  > "${cred_out}" 2>/dev/null

# Every KAFKA_SASL_PASSWORD / KAFKA_SASL_USERNAME occurrence in an env list
# must be followed by valueFrom, never by `value:`.
if grep -A1 -E 'name: KAFKA_SASL_(PASSWORD|USERNAME)$' "${cred_out}" | grep -qE '^\s+value:'; then
  echo "ASSERT FAIL  a SASL credential is rendered as a literal value"
  FAILED=$((FAILED + 1))
else
  echo "ok  SASL credentials render as secretKeyRef in every reconciler Job"
  PASSED=$((PASSED + 1))
fi

# ...and nothing secret may be in the ConfigMap the Jobs mount. The scripts
# in there legitimately contain the printf TEMPLATE `password="%s"`, which
# is why the pattern excludes a '%' immediately after the quote: anything
# else there is a literal that should not exist.
if awk '/kind: ConfigMap/,/^---/' "${cred_out}" | grep -qE 'password="[^%]'; then
  echo "ASSERT FAIL  a JAAS password is baked into the admin ConfigMap"
  FAILED=$((FAILED + 1))
else
  echo "ok  the admin ConfigMap carries no credential"
  PASSED=$((PASSED + 1))
fi

# ---- invariant: the §4.2 numbers survive every profile ------------------
# The registry is values, so it is overridable — but the two shipped
# production-shaped profiles must carry the §4.2 table verbatim, and the
# §7.2 coordination topic must be in every profile (Helm REPLACES lists, so
# an overlay that redefines `registry` can silently drop it).
echo
echo "asserting the §4.2 registry"
for profile in "" "-f ${CHART}/values-prod.yaml"; do
  label="${profile:-values.yaml}"
  # shellcheck disable=SC2086
  spec="$(helm template deps "${CHART}" ${profile} 2>/dev/null \
    | sed -n '/^  topics.txt:/,/^  [a-z]/p')"
  bad=0
  for want in \
    "messages.v1 512 86400000" \
    "flagged.v1 128 604800000" \
    "deletions.v1 128 86400000" \
    "usage.v1 32 604800000"
  do
    printf '%s\n' "${spec}" | grep -qF "${want}" || { echo "  MISSING: ${want}"; bad=1; }
  done
  if [ "${bad}" -eq 0 ]; then
    echo "ok  §4.2 partition counts and retentions intact  (${label})"
    PASSED=$((PASSED + 1))
  else
    echo "ASSERT FAIL  §4.2 registry drifted  (${label})"
    FAILED=$((FAILED + 1))
  fi
done

for profile in "" "-f ${CHART}/values-prod.yaml" "-f ${CHART}/values-local.yaml"; do
  label="${profile:-values.yaml}"
  # shellcheck disable=SC2086
  if helm template deps "${CHART}" ${profile} 2>/dev/null | grep -q 'adapter.shards.v1 '; then
    echo "ok  §7.2 adapter.shards.v1 present                (${label})"
    PASSED=$((PASSED + 1))
  else
    echo "ASSERT FAIL  adapter.shards.v1 missing from the registry  (${label})"
    FAILED=$((FAILED + 1))
  fi
done

echo
echo "passed=${PASSED} failed=${FAILED}"
[ "${FAILED}" -eq 0 ]
