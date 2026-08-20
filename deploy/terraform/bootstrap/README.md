# `bootstrap` — the remote state backend

Run once per account, before anything in `examples/`.

```sh
tofu init
tofu apply -var region=eu-west-1 -var state_bucket_name=dabet-tfstate-<account-id>
tofu output backend_config
```

## Chicken and egg

This root creates the bucket the other roots keep their state in, so it cannot
itself use that bucket. Two honest options:

1. **Leave it on local state** and keep `terraform.tfstate` somewhere safe. The
   resources here are a bucket and (optionally) a table; if the state is ever
   lost, `tofu import` recovers it in two commands.
2. **Migrate it in afterwards**: add a `backend "s3"` block pointing at the
   bucket it just created and run `tofu init -migrate-state`.

Both are fine. The first is simpler and is what most teams do.

## Why there is no DynamoDB table by default

The S3 backend has supported native locking since OpenTofu 1.10 and Terraform
1.10 (`use_lockfile = true`): it writes a `.tflock` object next to the state and
relies on S3's conditional writes. The DynamoDB table was only ever a workaround
for S3 not having conditional writes, and S3 has them now — so the table is a
second resource, a second IAM surface and a second bill for nothing.

`create_dynamodb_lock_table` is there for teams pinned to an older CLI. If you
enable it, do not assume the two mechanisms interlock: a run using the lock file
will not see a lock held in DynamoDB, or the reverse. Pick one.

## Deliberate differences from the rest of this directory

**Versioning is on**, unlike on the embeddings bucket. A state file is
overwritten on every apply, and a corrupted or truncated write is precisely the
failure a previous version recovers from. Superseded versions expire after a
year — state files are small and the history is worth more than the storage.

**SSE-S3, not a customer-managed key.** The key that would protect the state
would itself be described by the state; a key policy mistake then locks you out
of the file that tells you how to fix it. AES256 keeps the recovery path simple.

Both buckets and the optional table carry `prevent_destroy`.
