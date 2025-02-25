# Resource: Creating a basic S3 bucket
resource "aws_s3_bucket" "mmybucket" {
  # Lembre-se: bucket_name precisa ser único globalmente
  bucket = "ifftest1"  
}
