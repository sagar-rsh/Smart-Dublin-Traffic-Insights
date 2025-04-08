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

resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name      = var.redshift_workgroup
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  publicly_accessible = true
}