# `examples/dev` — a small, destroyable dabet on AWS

What a team actually edits: `main.tf` is the whole configuration, and every
value in it is either a size or one of three deliberate departures from the §3
target.

```sh
cp backend.hcl.example backend.hcl && $EDITOR backend.hcl
tofu init -backend-config=backend.hcl
tofu plan  -var 'cluster_admin_principal_arns=["arn:aws:iam::<account>:role/YourRole"]'
tofu apply
```

Then fill in the secret document and hand the contract to the charts:

```sh
tofu output -raw app_secret_document_skeleton > /tmp/app.json
tofu output -json postgres_password_commands
$EDITOR /tmp/app.json
aws secretsmanager put-secret-value --secret-id dabet/dev/app \
  --secret-string file:///tmp/app.json
shred -u /tmp/app.json

eval "$(tofu output -raw kubeconfig_command)"
tofu output -raw helm_values_dabet_deps_yaml > /tmp/deps.yaml
tofu output -raw helm_values_dabet_yaml      > /tmp/values.yaml

helm upgrade --install dabet-deps ../../../k8s/charts/dabet-deps \
  --namespace dabet --create-namespace -f /tmp/deps.yaml
helm upgrade --install dabet ../../../k8s/charts/dabet \
  --namespace dabet \
  -f ../../../k8s/charts/dabet/values-aws.yaml -f /tmp/values.yaml
```

The §4.2 topics are not created by any of that — the deps chart's topics Job is
gated on an in-cluster Kafka. Create them against the MSK bootstrap string
before the services start.

## What is different from prod, and why

**Size**, mostly: two `m7i.xlarge` nodes instead of twelve `m7i.2xlarge`, three
`kafka.t3.small` brokers instead of six `kafka.m7g.2xlarge`, a single-AZ
`db.t4g.medium` instead of two Multi-AZ Graviton instances, one NAT gateway
instead of three, and no interface VPC endpoints (the S3 *gateway* endpoint is
still created — that one is free and matters at any size).

**One Postgres instance, not two.** §3 asks for separate identity and policy
clusters, but §6.3's `policies.creator_id REFERENCES creators(id)` foreign key
cannot span two RDS instances. Collapsing them in dev keeps the constraint
enforced by the database, which is the more faithful thing to develop against.

**Two settings that are cost decisions, not capability ones.** Redis runs in
non-cluster mode on one `cache.t4g.micro`, and transit encryption is off on the
caches. Neither is working around a missing feature — `moderation-service`
builds a cluster-aware, TLS-capable client when asked — they are just what a
development environment needs. §4.3's hash tags mean flipping to the prod shape
is a flag.

Kafka is the exception: dev uses the same SASL/SCRAM over TLS that prod does,
because the credential path is the part worth exercising before production.

**Destroyability.** `force_destroy = true` on the buckets,
`deny_object_deletion = false`, `skip_final_snapshot = true`,
`deletion_protection = false`, and a 14-day expiration on the embeddings corpus
so a forgotten load test does not accrue forever. Every one of those is the
opposite in prod, where §4.8's 24-hour Kafka retention means an embedding
deleted by accident cannot be recomputed.

## Cost

Roughly **$500–700/month**, dominated by the two EKS nodes (~$290), the three
MSK brokers (~$130) and the EKS control plane (~$75). Not free, and not close to
free — there is no configuration of this topology that costs nothing while
running. `tofu destroy` between sessions is the real lever; everything here is
built to survive it.
