import os
import requests
import logging
import zipfile
import boto3
from botocore.exceptions import ClientError
import shutil

# Configura o logging para exibir mensagens no console
logging.basicConfig(level=logging.INFO)

def download_and_extract_zip(url: str, destination_folder: str) -> str:
    """
    Baixa um arquivo ZIP a partir de uma URL, extrai seu conteúdo em um diretório de destino
    e, após a extração, exclui o arquivo ZIP baixado.
    
    Args:
        url (str): URL do arquivo ZIP a ser baixado.
        destination_folder (str): Caminho do diretório onde o arquivo será salvo e extraído.
        
    Returns:
        str: O caminho do diretório onde o conteúdo foi extraído.
    """
    os.makedirs(destination_folder, exist_ok=True)
    
    # Define o caminho completo para salvar o arquivo ZIP
    zip_file_path = os.path.join(destination_folder, os.path.basename(url))
    
    logging.info(f"Iniciando download do arquivo: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        if os.path.exists(zip_file_path):
            logging.info(f"O arquivo {zip_file_path} já existe e será reescrito.")
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Arquivo baixado e salvo em: {zip_file_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao baixar o arquivo: {e}")
        raise
    
    # Define a pasta de extração dentro do diretório de destino
    extract_folder = os.path.join(destination_folder, "extracted")
    os.makedirs(extract_folder, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        logging.info(f"Conteúdo extraído para: {extract_folder}")
        
        # Remove o arquivo ZIP baixado
        os.remove(zip_file_path)
        logging.info("Arquivo ZIP removido com sucesso.")
    except zipfile.BadZipFile as e:
        logging.error(f"Erro ao extrair o arquivo ZIP: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro ao remover o arquivo ZIP: {e}")
        raise

    return extract_folder

def upload_folder_to_s3(bucket_name: str, folder_path: str, s3_folder: str = "", strip_levels: int = 2) -> None:
    """
    Faz upload recursivo de todos os arquivos de um diretório local para um bucket S3.
    Remove os primeiros `strip_levels` níveis do caminho relativo antes de criar a chave no S3.
    
    Args:
        bucket_name (str): Nome do bucket S3 de destino.
        folder_path (str): Caminho do diretório local a ser enviado.
        s3_folder (str, optional): Pasta de destino dentro do bucket. Se vazio, os arquivos serão enviados para a raiz.
        strip_levels (int, optional): Número de níveis de diretórios a serem removidos do caminho relativo.
    """
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("Credenciais AWS não definidas nas variáveis de ambiente.")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )
    
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)
            # Calcula o caminho relativo a partir de folder_path
            rel_path = os.path.relpath(local_path, folder_path)
            # Divide o caminho em partes e remove os primeiros `strip_levels` níveis
            parts = rel_path.split(os.sep)
            if len(parts) > strip_levels:
                stripped_path = os.path.join(*parts[strip_levels:])
            else:
                stripped_path = os.path.basename(local_path)
            s3_path = os.path.join(s3_folder, stripped_path) if s3_folder else stripped_path
            
            try:
                logging.info(f"Enviando {local_path} para s3://{bucket_name}/{s3_path}")
                s3_client.upload_file(local_path, bucket_name, s3_path)
            except ClientError as e:
                logging.error(f"Erro ao enviar {local_path} para s3://{bucket_name}/{s3_path}: {e}")
                raise

if __name__ == "__main__":
    # URL do arquivo ZIP a ser baixado
    url = "https://storage.googleapis.com/playoffs/iff/data_engineering.zip"
    
    # Diretório temporário local para o download e extração
    temp_folder = "/tmp/landing"
    
    # Realiza o download e extração; retorna o caminho onde o conteúdo foi extraído
    extracted_folder = download_and_extract_zip(url, temp_folder)
    
    # Nome do bucket S3 (por exemplo, 'iffdatatest')
    bucket_name = os.getenv('BUCKET_NAME')
    # Pasta de destino dentro do bucket (definida como "landing")
    s3_folder = "landing/"
    
    # Faz o upload recursivo do conteúdo extraído para o bucket S3, removendo os 2 primeiros níveis de diretórios
    upload_folder_to_s3(bucket_name, extracted_folder, s3_folder, strip_levels=2)
    
    # Remove o diretório temporário local após o upload
    shutil.rmtree(temp_folder)
    logging.info("Upload completo e diretório temporário removido.")
