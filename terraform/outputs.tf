output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "redshift_namespace_id" {
  value = aws_redshiftserverless_namespace.this.id
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.this.endpoint
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift_s3_access.arn
}