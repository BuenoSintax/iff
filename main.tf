
# Configuração do provedor AWS com credenciais embutidas
provider "aws" {
  region     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")         # ajuste a região conforme necessário
  access_key = os.getenv("AWS_ACCESS_KEY_ID")
  secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
}

# Recurso: Criação de um bucket S3 básico
resource "aws_s3_bucket" "meu_bucket" {
  bucket = "nome-unico-do-bucket-12345"  # O nome deve ser único globalmente
}
