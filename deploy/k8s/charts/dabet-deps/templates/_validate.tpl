{{/*
Fail at render time on value combinations that would otherwise produce a
release that installs cleanly and then does not work. Every check here
corresponds to something that is silent at `kubectl apply` time and loud
only hours later.
*/}}
{{- define "dabet-deps.validate" -}}

{{- if .Values.kafka.enabled }}
  {{- if lt (.Values.kafka.replicas | int) 1 }}
    {{- fail "kafka.replicas must be >= 1" }}
  {{- end }}
  {{- if and (gt (.Values.kafka.replicas | int) 1) (eq (mod (.Values.kafka.replicas | int) 2) 0) }}
    {{- /* KRaft's controller quorum is Raft: an even voter count gives the
           same fault tolerance as count-1 while costing one more node. Not
           fatal, but almost always a mistake. */}}
    {{- fail (printf "kafka.replicas=%d: the KRaft controller quorum needs an ODD number of voters (3, 5, 7). An even count tolerates no more failures than count-1." (.Values.kafka.replicas | int)) }}
  {{- end }}
  {{- if gt (.Values.kafka.minInsyncReplicas | int) (.Values.kafka.replicas | int) }}
    {{- fail (printf "kafka.minInsyncReplicas=%d exceeds kafka.replicas=%d: every produce with acks=all would fail with NOT_ENOUGH_REPLICAS" (.Values.kafka.minInsyncReplicas | int) (.Values.kafka.replicas | int)) }}
  {{- end }}
{{- end }}

{{/*
The topic registry is checked whether or not this chart runs the brokers:
the reconciler is no longer gated on kafka.enabled, so a bad registry is
just as reachable on the MSK path.
*/}}
{{- range .Values.kafka.topics.registry }}
  {{- if lt (.partitions | int) 1 }}
    {{- fail (printf "kafka topic %s: partitions must be >= 1" .name) }}
  {{- end }}
  {{- if lt (.retentionMs | int) 1 }}
    {{- fail (printf "kafka topic %s: retentionMs must be > 0 (docs §4.2 assigns each topic a finite retention)" .name) }}
  {{- end }}
{{- end }}

{{- if .Values.kafka.topics.enabled }}
  {{- if lt (include "dabet-deps.kafkaTopicRF" . | int) 1 }}
    {{- fail "kafka.topics.replicationFactor must be >= 1" }}
  {{- end }}
{{- end }}

{{/*
Credentials are a secretKeyRef and nothing else. Catching the missing
Secret name here turns "the reconciler Job CrashLoopBackOffs against a
broker that rejected it" into a render-time message naming the key.
*/}}
{{- $sasl := .Values.kafka.admin.auth.sasl }}
{{- if and $sasl.mechanism (not $sasl.existingSecret.name) }}
  {{- fail "kafka.admin.auth.sasl.mechanism is set but kafka.admin.auth.sasl.existingSecret.name is empty: the topic and ACL reconcilers take their credential from an existing Secret by reference, never as a value." }}
{{- end }}
{{- if and $sasl.mechanism (not .Values.kafka.admin.auth.tls.enabled) }}
  {{- /* Not fatal: SASL_PLAINTEXT is a legitimate configuration on a trusted
         network, and PLAIN over it is the classic mistake rather than a
         chart bug. But SCRAM without TLS on a managed broker never works —
         every managed Kafka requires TLS — so say so loudly. */}}
  {{- if hasPrefix "b-" (include "dabet-deps.kafkaAdminBootstrap" .) }}
    {{- fail "kafka.admin.auth.sasl.mechanism is set against what looks like an MSK bootstrap string but kafka.admin.auth.tls.enabled is false. MSK offers SASL only over TLS (port 9096); the connection will be refused." }}
  {{- end }}
{{- end }}

{{/*
An ACL rule with no operations is a binding that grants nothing and, worse,
still counts as "this resource has an ACL" — which on a broker with
allow.everyone.if.no.acl.found=true flips that resource from open to closed
for everyone. Silent, and exactly the kind of thing that shows up as an
unexplained authorization failure hours later.
*/}}
{{- if .Values.kafka.acls.enabled }}
  {{- range $i, $r := .Values.kafka.acls.rules }}
    {{- if not $r.principal }}
      {{- fail (printf "kafka.acls.rules[%d]: principal is required (e.g. \"User:dabet-moderation-service\")" $i) }}
    {{- end }}
    {{- if not (has $r.resourceType (list "cluster" "topic" "group" "transactional-id")) }}
      {{- fail (printf "kafka.acls.rules[%d] (%s): resourceType must be cluster, topic, group or transactional-id, got %q" $i $r.principal ($r.resourceType | toString)) }}
    {{- end }}
    {{- if and (ne $r.resourceType "cluster") (not $r.resourceName) }}
      {{- fail (printf "kafka.acls.rules[%d] (%s): resourceName is required for resourceType %s" $i $r.principal $r.resourceType) }}
    {{- end }}
    {{- if not $r.operations }}
      {{- fail (printf "kafka.acls.rules[%d] (%s %s): operations must not be empty — an ACL with no operation grants nothing while still closing the resource to everyone else" $i $r.principal $r.resourceType) }}
    {{- end }}
  {{- end }}
{{- end }}

{{- if .Values.postgres.enabled }}
  {{- range $name, $inst := .Values.postgres.instances }}
    {{- if $inst.enabled }}
      {{- if not $inst.database }}
        {{- fail (printf "postgres.instances.%s.database must be set" $name) }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

{{- if and .Values.redis.enabled .Values.redis.cluster.enabled }}
  {{- $n := .Values.redis.cluster.replicas | int }}
  {{- $rpm := .Values.redis.cluster.replicasPerMaster | int }}
  {{- if ne (mod $n (add1 $rpm)) 0 }}
    {{- fail (printf "redis.cluster.replicas=%d is not divisible by (1 + replicasPerMaster)=%d; redis-cli --cluster create would refuse" $n (add1 $rpm)) }}
  {{- end }}
  {{- $masters := div $n (add1 $rpm) }}
  {{- if lt $masters 3 }}
    {{- fail (printf "redis.cluster: %d master(s) — Redis Cluster requires at least 3 to cover the 16384 slots with a working quorum. Set redis.cluster.enabled=false for a single node instead of pretending." $masters) }}
  {{- end }}
{{- end }}

{{- if .Values.clickhouse.enabled }}
  {{- $want := mul (.Values.clickhouse.shards | int) (.Values.clickhouse.replicasPerShard | int) }}
  {{- if ne $want (.Values.clickhouse.replicas | int) }}
    {{- fail (printf "clickhouse: shards(%d) * replicasPerShard(%d) = %d but replicas = %d; the <remote_servers> topology would not match the running pods" (.Values.clickhouse.shards | int) (.Values.clickhouse.replicasPerShard | int) $want (.Values.clickhouse.replicas | int)) }}
  {{- end }}
  {{- if and (gt (.Values.clickhouse.replicasPerShard | int) 1) (not .Values.clickhouse.keeper.enabled) }}
    {{- fail "clickhouse.replicasPerShard > 1 requires clickhouse.keeper.enabled=true — ReplicatedMergeTree has nowhere to coordinate without Keeper" }}
  {{- end }}
  {{- if not .Values.clickhouse.auth.password }}
    {{- /* Not fatal: the chart generates one. But the username must exist,
           because the image disables `default` over the network as soon as
           CLICKHOUSE_USER is set and leaves /ping answering 200 regardless. */}}
    {{- if not .Values.clickhouse.auth.username }}
      {{- fail "clickhouse.auth.username must be set: the official image disables network access for `default` whenever CLICKHOUSE_USER/PASSWORD are absent, while /ping keeps reporting healthy" }}
    {{- end }}
  {{- end }}
{{- end }}

{{- if .Values.milvus.enabled }}
  {{- if not (has .Values.milvus.mode (list "standalone" "distributed")) }}
    {{- fail (printf "milvus.mode must be standalone or distributed, got %q" .Values.milvus.mode) }}
  {{- end }}
  {{- if and (eq .Values.milvus.mode "distributed") (eq .Values.milvus.messageQueue "kafka") (not (include "dabet-deps.kafkaBrokers" .)) }}
    {{- fail "milvus.messageQueue=kafka but no Kafka is available: enable kafka, set external.kafka.brokers, or switch milvus.messageQueue" }}
  {{- end }}
  {{- if not (include "dabet-deps.s3Endpoint" .) }}
    {{- fail "milvus needs an object store: enable minio or set external.s3.endpoint" }}
  {{- end }}
  {{- if not .Values.milvus.etcd.enabled }}
    {{- fail "milvus.etcd.enabled=false is not supported by this chart; Milvus requires an etcd for metadata" }}
  {{- end }}
{{- end }}

{{- if and .Values.vllm.enabled (lt (.Values.vllm.gpu.count | int) 1) }}
  {{- fail "vllm.gpu.count must be >= 1; vLLM does not run without an accelerator. Use mocks.llm instead." }}
{{- end }}

{{- if and .Values.vllm.enabled .Values.mocks.llm.enabled }}
  {{- /* Both can exist, but only one wins VLLM_ENDPOINT and the loser is
         a pod nobody talks to. */}}
  {{- fail "both vllm.enabled and mocks.llm.enabled are true: VLLM_ENDPOINT can only point at one of them. Disable mocks.llm when running the real fleet." }}
{{- end }}

{{- if and .Values.embedding.enabled .Values.mocks.embedding.enabled }}
  {{- fail "both embedding.enabled and mocks.embedding.enabled are true: EMBEDDING_ENDPOINT can only point at one of them. Disable mocks.embedding when running the real embedder." }}
{{- end }}

{{- if .Values.embedding.enabled }}
  {{- if not (has .Values.embedding.backend (list "tei" "vllm")) }}
    {{- fail (printf "embedding.backend must be tei or vllm, got %q" .Values.embedding.backend) }}
  {{- end }}
  {{- if ne (.Values.embedding.dimensions | int) 384 }}
    {{- /* §8.4 fixes 384 dimensions, and Milvus's collection schema is
           built to match. A mismatch is only discovered on the first
           insert, as an opaque Milvus error. */}}
    {{- fail (printf "embedding.dimensions=%d but docs §8.4 fixes the corpus at 384 dimensions; changing it invalidates every vector already in S3 and Milvus" (.Values.embedding.dimensions | int)) }}
  {{- end }}
{{- end }}
{{- end -}}
