resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Name = "SmartDublin trips Data Lake"
  }
}

resource "aws_iam_role" "redshift_s3_access" {
  name = "RedshiftS3AccessRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_access" {
  role       = aws_iam_role.redshift_s3_access.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = var.redshift_namespace
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_access.arn]
}

data "aws_vpc" "default" {
  default = true
}

# Create a security group allowing inbound access from dbt Cloud IPs
resource "aws_security_group" "redshift_dbt_cloud_access" {
  name        = "redshift-dbt-cloud-access"
  description = "Allow dbt Cloud IPs to access Redshift"
  vpc_id      = data.aws_vpc.default.id

  tags = {
    Name = "Redshift Dbt Cloud Access"
  }
}

# Inbound rules for each dbt Cloud IP address (Redshift port 5439)
resource "aws_security_group_rule" "dbt_inbound_rules" {
  for_each = toset([
    "52.45.144.63",
    "54.81.134.249",
    "52.22.161.231",
    "52.3.77.232",
    "3.214.191.130",
    "34.233.79.135"
  ])

  type              = "ingress"
  from_port         = 5439
  to_port           = 5439
  protocol          = "tcp"
  cidr_blocks       = [format("%s/32", each.key)]
  security_group_id = aws_security_group.redshift_dbt_cloud_access.id
}

# Attach the security group to the Redshift Serverless workgroup
resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name      = var.redshift_workgroup
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  publicly_accessible = true

  # Attach security group
  security_group_ids = [aws_security_group.redshift_dbt_cloud_access.id]
}
