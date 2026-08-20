{{/*
Shared helpers. Every template that iterates the nine services calls into
here, so naming, labelling, image resolution and the two safety checks
(grace period vs Kafka drain, replica ceiling vs partition count) exist in
exactly one place.
*/}}

{{- define "dabet.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dabet.fullname" -}}
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

{{- define "dabet.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Per-workload object name: <fullname>-<service>, e.g. dabet-moderation-service.
Args: dict "root" $ "name" <service key>
*/}}
{{- define "dabet.svcName" -}}
{{- printf "%s-%s" (include "dabet.fullname" .root) .name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels on every object. Args: dict "root" $ "name" <service key> (name optional).
*/}}
{{/*
Built as a dict and rendered once, rather than as literal lines with
commonLabels appended: appending would emit a DUPLICATE key whenever a user
sets one of the standard labels in global.commonLabels, and a duplicate key
in a Kubernetes manifest is a parse error at apply time, not at render time.
Merging makes the chart's own values win, which is the safe direction — the
selector labels are a subset of these and cannot be allowed to drift.
*/}}
{{- define "dabet.labels" -}}
{{- $std := dict
      "helm.sh/chart" (include "dabet.chart" .root)
      "app.kubernetes.io/name" (include "dabet.name" .root)
      "app.kubernetes.io/instance" .root.Release.Name
      "app.kubernetes.io/version" (.root.Chart.AppVersion | toString)
      "app.kubernetes.io/managed-by" .root.Release.Service
      "app.kubernetes.io/part-of" "dabet" -}}
{{- if .name -}}
{{- $_ := set $std "app.kubernetes.io/component" .name -}}
{{- end -}}
{{- toYaml (merge $std (deepCopy (default (dict) .root.Values.global.commonLabels))) -}}
{{- end -}}

{{/*
Selector labels — the immutable subset. Never add anything version-bearing
here: it is a Deployment/StatefulSet selector and cannot be changed in place.
*/}}
{{- define "dabet.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dabet.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{- define "dabet.annotations" -}}
{{- with .root.Values.global.commonAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Image reference. Per-service image.* overrides global.image.*.
Args: dict "root" $ "name" <service key> "svc" <service values>
*/}}
{{- define "dabet.image" -}}
{{- $g := .root.Values.global.image -}}
{{- $o := default (dict) .svc.image -}}
{{- $registry := default $g.registry $o.registry -}}
{{- $repo := default $g.repository $o.repository -}}
{{- $tag := default (default .root.Chart.AppVersion $g.tag) $o.tag -}}
{{- $path := printf "%s/%s" $repo .name -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" (trimSuffix "/" $registry) $path $tag -}}
{{- else -}}
{{- printf "%s:%s" $path $tag -}}
{{- end -}}
{{- end -}}

{{- define "dabet.imagePullPolicy" -}}
{{- $o := default (dict) .svc.image -}}
{{- default .root.Values.global.image.pullPolicy $o.pullPolicy -}}
{{- end -}}

{{/*
The one Secret every workload references, whatever fills it: the plain
Secret this chart renders, an ESO ExternalSecret, or a CSI SecretProviderClass
with syncSecret. Keeping the NAME stable is what makes those interchangeable.
*/}}
{{- define "dabet.secretName" -}}
{{- default (printf "%s-secrets" (include "dabet.fullname" .)) .Values.secrets.name -}}
{{- end -}}

{{- define "dabet.serviceAccountName" -}}
{{- if .root.Values.serviceAccount.create -}}
{{- include "dabet.svcName" (dict "root" .root "name" .name) -}}
{{- else -}}
{{- default "default" (default (dict) .svc.serviceAccount).name -}}
{{- end -}}
{{- end -}}

{{/*
Shared / per-service ConfigMap names.
*/}}
{{- define "dabet.sharedConfigName" -}}
{{- printf "%s-config" (include "dabet.fullname" .) -}}
{{- end -}}

{{- define "dabet.svcConfigName" -}}
{{- printf "%s-config" (include "dabet.svcName" (dict "root" .root "name" .name)) -}}
{{- end -}}

{{/*
--------------------------------------------------------------------------
Safety check 1 — terminationGracePeriodSeconds vs KAFKA_CONSUMER_DRAIN_TIMEOUT.

On SIGTERM a consumer revokes its partitions and waits up to DrainTimeout for
in-flight handlers, and pkg/service.Run then gives the HTTP servers 10s to
close. If the grace period is shorter than drain + shutdown, the kubelet
SIGKILLs the pod with handlers still running: those records were never
committed, so they are redelivered and re-processed. At-least-once tolerates
that (P3), but it is silent duplicated work on every single rollout, and the
usual "fix" is to shorten the grace period further, which makes it worse.

Rendering fails rather than shipping that. Args: dict "root" $ "name" "svc".
--------------------------------------------------------------------------
*/}}
{{- define "dabet.gracePeriod" -}}
{{- $svc := .svc -}}
{{- $grace := int (default 30 $svc.terminationGracePeriodSeconds) -}}
{{- if (default (dict) $svc.consumer).enabled -}}
{{- $drain := int .root.Values.kafka.consumer.drainTimeoutSeconds -}}
{{- $floor := add $drain 10 -}}
{{- if le $grace (int $floor) -}}
{{- fail (printf "services.%s.terminationGracePeriodSeconds is %d, which is not greater than kafka.consumer.drainTimeoutSeconds (%d) plus the runner's 10s HTTP shutdown. In-flight Kafka records would be killed mid-handler on every rollout. Raise the grace period (or lower the drain timeout) - do not silence this." .name $grace $drain) -}}
{{- end -}}
{{- end -}}
{{- $grace -}}
{{- end -}}

{{/*
--------------------------------------------------------------------------
Safety check 2 — the partition ceiling.

A consumer group cannot usefully exceed the partition count of the topic it
consumes: surplus members join the group, are assigned nothing, and idle.
§4.2 sets messages.v1 at 512, flagged.v1 and deletions.v1 at 128, usage.v1
at 32. maxReplicas is clamped to that number for every consumer, so a
misconfigured autoscaler cannot burn money on members that will never own a
partition.

Args: dict "root" $ "name" "svc"   Returns the effective maxReplicas.
--------------------------------------------------------------------------
*/}}
{{- define "dabet.maxReplicas" -}}
{{- $svc := .svc -}}
{{- $as := default (dict) $svc.autoscaling -}}
{{- $max := int (default 10 $as.maxReplicas) -}}
{{- $consumer := default (dict) $svc.consumer -}}
{{- if $consumer.enabled -}}
{{- $topic := index .root.Values.kafka.topics $consumer.topic -}}
{{- $partitions := int $topic.partitions -}}
{{- if gt $max $partitions -}}
{{- $max = $partitions -}}
{{- end -}}
{{- end -}}
{{- $max -}}
{{- end -}}

{{- define "dabet.consumerTopicName" -}}
{{- $consumer := default (dict) .svc.consumer -}}
{{- (index .root.Values.kafka.topics $consumer.topic).name -}}
{{- end -}}

{{/*
Which autoscaler a service gets: its own mode, else the global one, else none.
*/}}
{{- define "dabet.autoscalingMode" -}}
{{- $as := default (dict) .svc.autoscaling -}}
{{- if not $as.enabled -}}
none
{{- else -}}
{{- $mode := default .root.Values.autoscaling.mode $as.mode -}}
{{- $consumer := default (dict) .svc.consumer -}}
{{- if and (eq $mode "keda") (not $consumer.enabled) -}}
{{/* No consumer group means no lag to read. Fall back rather than render a
     ScaledObject whose trigger can never fire. */}}
hpa
{{- else -}}
{{- $mode -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Does this service verify or issue a JWT? Args: dict "root" $ "svc".
Only these mount RS256 key material.
*/}}
{{- define "dabet.usesJWT" -}}
{{- $mode := default "none" .svc.jwt -}}
{{- if or (eq $mode "verify") (eq $mode "issue") -}}true{{- end -}}
{{- end -}}

{{- define "dabet.rs256" -}}
{{- if eq (upper .Values.jwt.alg) "RS256" -}}true{{- end -}}
{{- end -}}

{{/*
Ingress hosts: the primary host plus any extras.
*/}}
{{- define "dabet.ingressHosts" -}}
{{- $hosts := list .Values.ingress.host -}}
{{- range .Values.ingress.extraHosts -}}
{{- $hosts = append $hosts . -}}
{{- end -}}
{{- toYaml $hosts -}}
{{- end -}}
