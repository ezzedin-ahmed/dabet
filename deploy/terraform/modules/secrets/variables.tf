variable "name_prefix" {
  description = <<-EOT
    Path prefix for every secret, e.g. "dabet/prod". Secret names become
    "<prefix>/<name>", which is what External Secrets Operator's role is
    scoped to.
  EOT
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key encrypting the secret values."
  type        = string
}

variable "recovery_window_days" {
  description = <<-EOT
    Days a deleted secret stays recoverable. Zero forces immediate deletion,
    which is only appropriate for throwaway environments — and note that a
    zero-window delete makes the name immediately reusable, whereas the default
    7-30 day window blocks recreating a secret with the same name.
  EOT
  type        = number
  default     = 30
}

variable "secrets" {
  description = <<-EOT
    The §4.4 secrets, as a map of short name to description. The default covers
    everything §4.4 lists: Postgres passwords (handled separately by RDS, see
    the README), the Stripe key, the OAuth client secrets, and the JWT RS256
    key pair.

    Values are NOT set here. See create_placeholder_versions.
  EOT
  type        = map(string)
  default = {
    "stripe/secret-key"     = "Stripe API secret key (§5.7). Plain string."
    "stripe/webhook-secret" = "Stripe webhook signing secret (§5.7). Plain string, whsec_..."
    "oauth/youtube"         = "YouTube OAuth client (§5.5). JSON: {client_id, client_secret}"
    "oauth/twitch"          = "Twitch OAuth client (§5.5). JSON: {client_id, client_secret}"
    "oauth/discord"         = "Discord bot credentials (§5.5, §7.2). JSON: {client_id, client_secret, bot_token}"
    "jwt/private-key"       = "RS256 signing key (§5.4). PEM, PKCS#8. Held only by user-service."
    "jwt/public-key"        = "RS256 verification key (§5.4). PEM. Readable by every service that validates a token."
    "clickhouse/password"   = "Password for the in-cluster ClickHouse user (§8.7). Plain string."
  }
}

variable "create_placeholder_versions" {
  description = <<-EOT
    Create an initial version holding a placeholder string.

    On by default, because External Secrets Operator errors on a secret with no
    version and a fresh environment would otherwise need a manual step before
    anything starts. The placeholder is not a credential, the resource ignores
    subsequent changes to secret_string, and populating the real value with
    `aws secretsmanager put-secret-value` does not produce a Terraform diff.

    Nothing in this module ever accepts a real secret value as an input. A
    variable holding one would be written to the state file in plaintext and to
    whatever CI system prints the plan.
  EOT
  type        = bool
  default     = true
}

variable "placeholder_value" {
  description = "The placeholder written into the first version. Must not be a real credential."
  type        = string
  default     = "REPLACE_ME"
}

variable "rotation" {
  description = <<-EOT
    Optional rotation per secret: a Lambda ARN and a day interval. Left empty,
    no automatic rotation is configured — which is honest, because rotating a
    Stripe key or a Discord bot token means calling that provider's API, and
    there is no generic Lambda that can do it.

    The one secret that IS rotated automatically is the Postgres master
    password, and that is RDS's own managed rotation, configured in the rds
    module rather than here.
  EOT
  type = map(object({
    lambda_arn               = string
    automatically_after_days = number
  }))
  default = {}
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
