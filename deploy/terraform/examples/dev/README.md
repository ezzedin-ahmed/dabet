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

Then populate the secret placeholders and hand the contract to the charts:

```sh
tofu output secret_arns
aws secretsmanager put-secret-value --secret-id dabet/dev/stripe/secret-key --secret-string sk_test_...
tofu output -raw helm_values_aws_yaml > ../../../k8s/values-aws.yaml
eval "$(tofu output -raw kubeconfig_command)"
```

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

**Three settings that are not size decisions.** Redis runs in non-cluster mode,
MSK runs unauthenticated over `TLS_PLAINTEXT`, and transit encryption is off on
the caches — not to save money, but because the application code cannot yet talk
to the alternatives. The details are in the root README under "Application
changes this topology needs". Dev is configured so a first bring-up works
against the code as it stands; prod is configured for the target and will not
work until those three changes land.

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
