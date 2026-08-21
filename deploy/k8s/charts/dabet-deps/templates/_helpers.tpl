{{/*
Naming and labelling.
*/}}

{{- define "dabet-deps.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dabet-deps.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "dabet-deps.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "dabet-deps.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dabet
{{- end -}}

{{/* componentLabels: pass (dict "root" $ "component" "kafka") */}}
{{- define "dabet-deps.componentLabels" -}}
{{ include "dabet-deps.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* selectorLabels: pass (dict "root" $ "component" "kafka") */}}
{{- define "dabet-deps.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dabet-deps.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Per-component resource name, e.g. "dabet-deps-kafka". */}}
{{- define "dabet-deps.componentName" -}}
{{- printf "%s-%s" (include "dabet-deps.fullname" .root) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cluster DNS suffix. Kubernetes' in-cluster resolver appends the search
domains, but StatefulSet peer discovery (Kafka's quorum voters, etcd's
initial cluster, Redis's announced hostname) must use fully-qualified names
because those strings are also parsed by the peers themselves.
*/}}
{{- define "dabet-deps.clusterDomain" -}}
{{- default "cluster.local" .Values.clusterDomain -}}
{{- end -}}

{{/* imagePullSecrets block, or nothing. */}}
{{- define "dabet-deps.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{ toYaml . | indent 2 }}
{{- end }}
{{- end -}}

{{/*
Image reference from a {repository, tag} dict.
*/}}
{{- define "dabet-deps.image" -}}
{{- printf "%s:%s" .repository (.tag | toString) -}}
{{- end -}}

{{/*
Pick a storage class: component override, then global, then the cluster
default. An empty string must be OMITTED, not emitted as "" — an explicit
empty storageClassName means "no dynamic provisioning" to Kubernetes, which
is emphatically not what a blank value in values.yaml means.
Call: include "dabet-deps.storageClass" (dict "root" $ "override" .storageClass)
*/}}
{{- define "dabet-deps.storageClass" -}}
{{- $sc := .override | default .root.Values.global.storageClass -}}
{{- if $sc }}
storageClassName: {{ $sc | quote }}
{{- end }}
{{- end -}}

{{/*
nodeSelector / tolerations merged with the globals.
Call: include "dabet-deps.scheduling" (dict "root" $ "nodeSelector" .. "tolerations" ..)
*/}}
{{- define "dabet-deps.scheduling" -}}
{{- $ns := merge (dict) (.nodeSelector | default dict) (.root.Values.global.nodeSelector | default dict) -}}
{{- if $ns }}
nodeSelector:
{{ toYaml $ns | indent 2 }}
{{- end }}
{{- $tol := concat (.tolerations | default list) (.root.Values.global.tolerations | default list) -}}
{{- if $tol }}
tolerations:
{{ toYaml $tol | indent 2 }}
{{- end }}
{{- end -}}

{{/*
Soft topology spread over nodes. ScheduleAnyway on purpose: a single-node
kind cluster must still schedule every replica, and a hard constraint would
leave all but one Pending with no useful error.
Call: include "dabet-deps.topologySpread" (dict "root" $ "component" "kafka")
*/}}
{{- define "dabet-deps.topologySpread" -}}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        {{- include "dabet-deps.selectorLabels" (dict "root" .root "component" .component) | nindent 8 }}
{{- end -}}

{{/* =====================================================================
     PASSWORD PERSISTENCE

     A generated password must survive `helm upgrade`. randAlphaNum is
     re-evaluated on every render, so a naive `randAlphaNum 24` would mint a
     new password on upgrade, write it to the Secret, and leave the running
     Postgres — whose PGDATA already has the old one — unreachable. So:
     read the live Secret first via lookup(), and only generate when there
     is nothing there. lookup() returns an empty dict during `helm template`
     and `--dry-run`, which is why rendered output is not byte-stable; that
     is expected and harmless.

     Call: include "dabet-deps.password"
             (dict "root" $ "secret" "name" "key" "password" "given" "")
     ===================================================================== */}}
{{- define "dabet-deps.password" -}}
{{- if .given -}}
{{- .given -}}
{{- else -}}
{{- $existing := lookup "v1" "Secret" .root.Release.Namespace .secret -}}
{{- if and $existing $existing.data (index $existing.data .key) -}}
{{- index $existing.data .key | b64dec -}}
{{- else -}}
{{- randAlphaNum 24 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* =====================================================================
     ENDPOINT RESOLUTION — the §4.4 contract.

     Every helper below follows the same rule:

        an explicit external.* value wins
        else, if the component is enabled, the in-cluster address
        else, the empty string

     "Empty string" is a deliberate, load-bearing outcome: it is what the
     app chart sees when a dependency is neither self-hosted nor configured,
     and it is what lets `helm template` succeed with any subset disabled.
     The connection Secret omits empty keys entirely so a consuming Deployment
     using secretKeyRef with optional:true simply gets no env var, and the
     service falls back to its own default (§4.4).
     ===================================================================== */}}

{{/* KAFKA_BROKERS */}}
{{- define "dabet-deps.kafkaBrokers" -}}
{{- if .Values.external.kafka.brokers -}}
{{- .Values.external.kafka.brokers -}}
{{- else if .Values.kafka.enabled -}}
{{- $svc := include "dabet-deps.componentName" (dict "root" . "component" "kafka-headless") -}}
{{- $port := .Values.kafka.listeners.clientPort | int -}}
{{- $out := list -}}
{{- range $i := until (.Values.kafka.replicas | int) -}}
{{- $out = append $out (printf "%s-%d.%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" $ "component" "kafka")) $i $svc $.Release.Namespace (include "dabet-deps.clusterDomain" $) $port) -}}
{{- end -}}
{{- join "," $out -}}
{{- end -}}
{{- end -}}

{{/*
POSTGRES_DSN for one instance.
Call: include "dabet-deps.postgresDSN" (dict "root" $ "name" "identity")
*/}}
{{- define "dabet-deps.postgresDSN" -}}
{{- $root := .root -}}
{{- $name := .name -}}
{{- $ext := index $root.Values.external.postgres $name | default "" -}}
{{- if $ext -}}
{{- $ext -}}
{{- else -}}
{{- $inst := index $root.Values.postgres.instances $name -}}
{{- if and $root.Values.postgres.enabled $inst.enabled -}}
{{- $d := $root.Values.postgres.defaults -}}
{{- $user := $inst.username | default $d.username -}}
{{- $secretName := printf "%s-postgres-%s" (include "dabet-deps.fullname" $root) $name -}}
{{- $pw := include "dabet-deps.password" (dict "root" $root "secret" $secretName "key" "password" "given" ($inst.password | default $d.password)) -}}
{{- $host := printf "%s.%s.svc.%s" (include "dabet-deps.componentName" (dict "root" $root "component" (printf "postgres-%s" $name))) $root.Release.Namespace (include "dabet-deps.clusterDomain" $root) -}}
{{- printf "postgres://%s:%s@%s:5432/%s?sslmode=disable" $user (urlquery $pw) $host $inst.database -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* REDIS_ADDR — a single host:port standalone, or a comma-separated seed list. */}}
{{- define "dabet-deps.redisAddr" -}}
{{- if .Values.external.redis.addr -}}
{{- .Values.external.redis.addr -}}
{{- else if .Values.redis.enabled -}}
{{- $sts := include "dabet-deps.componentName" (dict "root" . "component" "redis") -}}
{{- $hs := include "dabet-deps.componentName" (dict "root" . "component" "redis-headless") -}}
{{- $dom := printf "%s.svc.%s" .Release.Namespace (include "dabet-deps.clusterDomain" .) -}}
{{- $port := .Values.redis.port | int -}}
{{- if .Values.redis.cluster.enabled -}}
{{- $out := list -}}
{{- range $i := until (.Values.redis.cluster.replicas | int) -}}
{{- $out = append $out (printf "%s-%d.%s.%s:%d" $sts $i $hs $dom $port) -}}
{{- end -}}
{{- join "," $out -}}
{{- else -}}
{{- printf "%s.%s:%d" $sts $dom $port -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Is the published Redis a cluster? Published as REDIS_CLUSTER. */}}
{{- define "dabet-deps.redisCluster" -}}
{{- if .Values.external.redis.addr -}}
{{- .Values.external.redis.cluster | toString -}}
{{- else if .Values.redis.enabled -}}
{{- .Values.redis.cluster.enabled | toString -}}
{{- else -}}
{{- "false" -}}
{{- end -}}
{{- end -}}

{{/*
MEMCADHED_ADDRS — the full pod list, not a ClusterIP.

gomemcache (and every other memcached client) hashes the key across the
address list to pick a node. Handing it one ClusterIP would put a random
node behind every request, so the same policy document would be cached on
all N nodes and invalidation would miss most of them (§6.8).
*/}}
{{- define "dabet-deps.memcachedAddrs" -}}
{{- if .Values.external.memcached.addrs -}}
{{- .Values.external.memcached.addrs -}}
{{- else if .Values.memcached.enabled -}}
{{- $sts := include "dabet-deps.componentName" (dict "root" . "component" "memcached") -}}
{{- $hs := include "dabet-deps.componentName" (dict "root" . "component" "memcached-headless") -}}
{{- $port := .Values.memcached.port | int -}}
{{- $out := list -}}
{{- range $i := until (.Values.memcached.replicas | int) -}}
{{- $out = append $out (printf "%s-%d.%s.%s.svc.%s:%d" $sts $i $hs $.Release.Namespace (include "dabet-deps.clusterDomain" $) $port) -}}
{{- end -}}
{{- join "," $out -}}
{{- end -}}
{{- end -}}

{{/* CLICKHOUSE_DSN — native protocol, matching the compose profile. */}}
{{- define "dabet-deps.clickhouseDSN" -}}
{{- if .Values.external.clickhouse.dsn -}}
{{- .Values.external.clickhouse.dsn -}}
{{- else if .Values.clickhouse.enabled -}}
{{- $secretName := printf "%s-clickhouse" (include "dabet-deps.fullname" .) -}}
{{- $pw := include "dabet-deps.password" (dict "root" . "secret" $secretName "key" "password" "given" .Values.clickhouse.auth.password) -}}
{{- $host := printf "%s.%s.svc.%s" (include "dabet-deps.componentName" (dict "root" . "component" "clickhouse")) .Release.Namespace (include "dabet-deps.clusterDomain" .) -}}
{{- printf "clickhouse://%s:%s@%s:%d/%s" .Values.clickhouse.auth.username (urlquery $pw) $host (.Values.clickhouse.ports.native | int) .Values.clickhouse.auth.database -}}
{{- end -}}
{{- end -}}

{{/* MILVUS_ADDR — host:port of the proxy (distributed) or the pod (standalone). */}}
{{- define "dabet-deps.milvusAddr" -}}
{{- if .Values.external.milvus.addr -}}
{{- .Values.external.milvus.addr -}}
{{- else if .Values.milvus.enabled -}}
{{- printf "%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "milvus")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.milvus.port | int) -}}
{{- end -}}
{{- end -}}

{{/* S3_ENDPOINT — empty means "real AWS S3, use the SDK default". */}}
{{- define "dabet-deps.s3Endpoint" -}}
{{- if .Values.external.s3.endpoint -}}
{{- .Values.external.s3.endpoint -}}
{{- else if .Values.minio.enabled -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "minio")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.minio.ports.api | int) -}}
{{- end -}}
{{- end -}}

{{- define "dabet-deps.s3Bucket" -}}
{{- if .Values.minio.enabled -}}
{{- .Values.minio.buckets.embeddings -}}
{{- else -}}
{{- .Values.external.s3.bucket -}}
{{- end -}}
{{- end -}}

{{/*
VLLM_ENDPOINT — external, then the real fleet, then the mock.
The mock is a substitute at the same OpenAI-compatible interface, so the
consuming service cannot tell the difference and needs no flag.
*/}}
{{- define "dabet-deps.vllmEndpoint" -}}
{{- if .Values.external.vllm.endpoint -}}
{{- .Values.external.vllm.endpoint -}}
{{- else if .Values.vllm.enabled -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "vllm")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.vllm.port | int) -}}
{{- else if .Values.mocks.llm.enabled -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "mockllm")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.mocks.llm.port | int) -}}
{{- end -}}
{{- end -}}

{{/* EMBEDDING_ENDPOINT — one deployment, one Service, shared by §7.4 and §8.4. */}}
{{- define "dabet-deps.embeddingEndpoint" -}}
{{- if .Values.external.embedding.endpoint -}}
{{- .Values.external.embedding.endpoint -}}
{{- else if .Values.embedding.enabled -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "embedding")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.embedding.port | int) -}}
{{- else if .Values.mocks.embedding.enabled -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "dabet-deps.componentName" (dict "root" . "component" "mockembed")) .Release.Namespace (include "dabet-deps.clusterDomain" .) (.Values.mocks.embedding.port | int) -}}
{{- end -}}
{{- end -}}

{{/* Effective Kafka replication factor, never above the broker count. */}}
{{- define "dabet-deps.kafkaRF" -}}
{{- min (.Values.kafka.replicationFactor | int) (.Values.kafka.replicas | int) -}}
{{- end -}}

{{/*
Is the broker the one THIS chart runs?

`kafka.replicas` is only a real broker count in that case. When an
`external.kafka.brokers` string is set — MSK, Confluent, anything — the
broker count is unknown to the chart, so capping the topic replication
factor at `kafka.replicas` (which still has its default of 3 sitting in
values.yaml) would silently invent a number. Everything that is only true
of the self-hosted StatefulSet keys off this.
*/}}
{{- define "dabet-deps.kafkaSelfHosted" -}}
{{- if and .Values.kafka.enabled (not .Values.external.kafka.brokers) -}}true{{- end -}}
{{- end -}}

{{/*
Replication factor for the §4.2 topics.

Self-hosted: capped at the broker count, so a one-broker laptop still
installs. External: taken verbatim, because the chart cannot know how many
brokers are out there and MSK's own default.replication.factor is 3.
*/}}
{{- define "dabet-deps.kafkaTopicRF" -}}
{{- if include "dabet-deps.kafkaSelfHosted" . -}}
{{- min (.Values.kafka.topics.replicationFactor | int) (.Values.kafka.replicas | int) -}}
{{- else -}}
{{- .Values.kafka.topics.replicationFactor | int -}}
{{- end -}}
{{- end -}}

{{/* min.insync.replicas can never exceed the replication factor. */}}
{{- define "dabet-deps.kafkaMinISR" -}}
{{- min (.Values.kafka.minInsyncReplicas | int) (include "dabet-deps.kafkaRF" . | int) -}}
{{- end -}}

{{/*
min.insync.replicas for the §4.2 TOPICS, which is not the same number as
the broker-level one once the broker is external: it is capped at the topic
replication factor above, not at the self-hosted quorum.

`kafka.topics.minInsyncReplicas` overrides `kafka.minInsyncReplicas` when
set, so an external cluster with a different durability contract does not
have to lie about the in-cluster broker settings to express it.
*/}}
{{- define "dabet-deps.kafkaTopicMinISR" -}}
{{- $want := .Values.kafka.minInsyncReplicas | int -}}
{{- if not (kindIs "invalid" .Values.kafka.topics.minInsyncReplicas) -}}
{{- $want = .Values.kafka.topics.minInsyncReplicas | int -}}
{{- end -}}
{{- min $want (include "dabet-deps.kafkaTopicRF" . | int) -}}
{{- end -}}

{{/*
=============================================================================
THE ADMIN CLIENT (topic reconciler + ACL reconciler)

Both Jobs talk to whatever broker the connection contract points at, which
is `external.kafka.brokers` when it is set and the in-cluster StatefulSet
otherwise — the SAME resolution the services get, so the reconciler can
never end up managing a different cluster from the one that is in use.
`kafka.admin.bootstrapOverride` is the escape hatch for the one case where
they legitimately differ: a broker that publishes a separate admin listener.
=============================================================================
*/}}

{{- define "dabet-deps.kafkaAdminBootstrap" -}}
{{- if .Values.kafka.admin.bootstrapOverride -}}
{{- .Values.kafka.admin.bootstrapOverride -}}
{{- else -}}
{{- include "dabet-deps.kafkaBrokers" . -}}
{{- end -}}
{{- end -}}

{{/* Image for the admin Jobs: kafka.image, with kafka.admin.image merged over it. */}}
{{- define "dabet-deps.kafkaAdminImage" -}}
{{- $img := mergeOverwrite (deepCopy .Values.kafka.image) (.Values.kafka.admin.image | default dict) -}}
{{- include "dabet-deps.image" $img -}}
{{- end -}}

{{- define "dabet-deps.kafkaAdminImagePullPolicy" -}}
{{- $img := mergeOverwrite (deepCopy .Values.kafka.image) (.Values.kafka.admin.image | default dict) -}}
{{- $img.pullPolicy -}}
{{- end -}}

{{/*
Environment for the admin Jobs.

The SASL username and password are the ONLY credentials involved and they
are always a secretKeyRef — never a literal, never a ConfigMap key. The
Job's entrypoint reads them from the environment and writes a
client.properties into a memory-backed emptyDir; nothing lands on disk and
nothing lands in the rendered manifest. `helm template | grep` on a release
of this chart will show the Secret NAME and never the value.
*/}}
{{- define "dabet-deps.kafkaAdminEnv" -}}
{{- $a := .Values.kafka.admin.auth -}}
- name: KAFKA_BOOTSTRAP
  value: {{ include "dabet-deps.kafkaAdminBootstrap" . | quote }}
- name: KAFKA_TLS_ENABLED
  value: {{ $a.tls.enabled | toString | quote }}
- name: KAFKA_TLS_SKIP_VERIFY
  value: {{ $a.tls.skipVerify | toString | quote }}
{{- if $a.tls.caSecret.name }}
- name: KAFKA_TLS_CA_FILE
  value: /kafka-ca/{{ $a.tls.caSecret.key }}
{{- end }}
- name: KAFKA_SASL_MECHANISM
  value: {{ $a.sasl.mechanism | quote }}
{{- if $a.sasl.mechanism }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ required "kafka.admin.auth.sasl.existingSecret.name is required when kafka.admin.auth.sasl.mechanism is set" $a.sasl.existingSecret.name | quote }}
      key: {{ $a.sasl.existingSecret.usernameKey | quote }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $a.sasl.existingSecret.name | quote }}
      key: {{ $a.sasl.existingSecret.passwordKey | quote }}
{{- end }}
{{- end -}}

{{/* Volume mounts shared by both admin Jobs. */}}
{{- define "dabet-deps.kafkaAdminVolumeMounts" -}}
- name: spec
  mountPath: /spec
- name: run
  mountPath: /run/kafka-admin
{{- if .Values.kafka.admin.auth.tls.caSecret.name }}
- name: kafka-ca
  mountPath: /kafka-ca
  readOnly: true
{{- end }}
{{- end -}}

{{- define "dabet-deps.kafkaAdminVolumes" -}}
{{- /* No defaultMode: the scripts are invoked as `bash /spec/<script>`, so
       they never need an execute bit, and an octal literal in YAML is a
       recurring source of 0555-read-as-555 bugs. */}}
- name: spec
  configMap:
    name: {{ include "dabet-deps.componentName" (dict "root" . "component" "kafka-admin") }}
- name: run
  emptyDir:
    medium: Memory
    sizeLimit: 1Mi
{{- if .Values.kafka.admin.auth.tls.caSecret.name }}
- name: kafka-ca
  secret:
    secretName: {{ .Values.kafka.admin.auth.tls.caSecret.name }}
{{- end }}
{{- end -}}
