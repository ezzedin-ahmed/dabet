output "vpc_id" {
  description = "VPC id."
  value       = aws_vpc.this.id
}

output "vpc_cidr_block" {
  description = "VPC CIDR, used to scope security group rules instead of 0.0.0.0/0."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnets, for internet-facing load balancers only."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnets for EKS nodes and pods."
  value       = aws_subnet.private[*].id
}

output "data_subnet_ids" {
  description = "Isolated data-tier subnets for RDS, MSK and ElastiCache."
  value       = aws_subnet.data[*].id
}

output "availability_zones" {
  description = "AZs this VPC spans, in the order the subnet lists use."
  value       = var.azs
}

output "vpc_endpoint_security_group_id" {
  description = "Security group attached to the interface VPC endpoints."
  value       = aws_security_group.endpoints.id
}

output "nat_gateway_public_ips" {
  description = <<-EOT
    Egress addresses. Register these with the platform providers if YouTube,
    Twitch or Discord ever require an allow-listed source, and note them for
    Stripe webhooks in the reverse direction.
  EOT
  value       = aws_eip.nat[*].public_ip
}
