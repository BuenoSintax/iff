# IFF Project

To achieve the proposed objective, we implemented a solution using the **Medallion Architecture**, processing nine sets of synthetic data representing various business entities such as customers, suppliers, revenues, and more. The system was designed to ingest, transform, and model this data across three distinct tiers—**Bronze**, **Silver**, and **Gold**—using modern, open-source tools. Below, we detail how this was accomplished with **Terraform**, **Airflow**, **Docker**, and **DuckDB**.

---

## Docker

**Docker** was essential for creating the virtual infrastructure. The project was developed on a macOS environment and virtualized on Linux using containers. This approach ensured:

- **Consistency** across different platforms
- **Portability** for easy deployment
- **Reproducibility** of the setup

---

## Terraform

**Terraform** was utilized as an infrastructure-as-code tool to provision the AWS S3 environment. This step is optional if you already have a test bucket. If not, you can:

1. Use a generic AWS account with access keys
2. Follow the accompanying documentation to create a test bucket

---

## Airflow

**Airflow** orchestrated workflows between the data lake and the analytical database, with all orchestration coded in Python. Key points include:

- Minimized pipelines for streamlined unit testing of ingestion and processing
- Recommendation: Subdivide pipelines further in larger-scale implementations for better **modularity** and **maintainability**

---

## DuckDB

**DuckDB** acted as the analytical database, managing the data warehouse processing layers:

- **Bronze Tier**: Handled data ingestion
- **Silver Tier**: Facilitated efficient data transformation
- **Gold Tier**: Enabled optimized data modeling

This setup ensured a robust and efficient data pipeline.

---



## Índice

- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
- [Exemplos](#exemplos)
- [Contribuição](#contribuição)
- [Licença](#licença)
- [Contato](#contato)

## Instalação

### Pré-requisitos

- Docker version 27.4.0, build bde2b89
- Python 3.12.9
- Dbeaver 24.1.4.202408041450 (Optional)
- Airflow 2.10.5
- Terraform 1.10.5
- Dependências externas, se houver.

### Initial Steps

Follow these instructions to install the project locally:

1. Clone the repository:
   ```bash
   git clone https://github.com/BuenoSintax/iff.git

For me, my local now is Users/mauriciobueno/iff
   

### Initial Steps for env

This step is for create a virtual environment with Docker. You can create image from docker file:

1. On your folder cloned, you will create a build of docker file:
   
   ```bash
   cd <your path here>/iff
   docker build -t infra_iff .

2. You need an AWS security credential to store the csv files. Access your AWS console log and search for IAM > Access keys. 
   Anote AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, be it what you created now or what you already have.
   
3. After this, you need to run a docker container. Make sure docker is running.
   This is an important step because it is where the AWS environment variables will be passed. To create security_credentials in AWS Console
   It needs to be in this folder because a docker volume will be created so that it is also possible to work with local files.
   Below you can see that some changes need to be made.

   Ensure that -v /Users/mauriciobueno/iff:/home/infra is your cloned folder, i.e. -v 'YourPathHere'/iff:/home/infra
   If you don't have a bucket, make sure it is unique. Otherwise, adopt the name of your existing bucket.
    
   ```bash
   docker run -d --name infra_iff -p 8080:8080 \
     -v /Users/mauriciobueno/iff:/home/infra \
     -e AWS_ACCESS_KEY_ID='Your Access Key' \
     -e AWS_SECRET_ACCESS_KEY='Your Secret Key' \
     -e AWS_DEFAULT_REGION=sa-east-1 \
     -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
     -e BUCKET_NAME='Your bucket' 
     infra_iff


## Use
After that, the container should be running and the folder should have the following characteristics.

<img width="444" alt="image" src="https://github.com/user-attachments/assets/bac4cc8e-2b6c-45f2-9570-75fe7555a166" />



With the local terminal in the /iff folder, you now need to access the container terminal.

```bash   
    docker exec -it infra_iff bash
```

A partir de agora já está funcionando o ambiente de desenvolvimento, caso já exista o bucket. Caso não exista, siga o procedimento abaixo.

Define the unique name in main.ts for the bucket, in my case I used:
<img width="884" alt="image" src="https://github.com/user-attachments/assets/ed6a662c-b386-425e-a6b8-40aa6c151266" />

On container terminal as exemple infra@c239b7356250:~$ , execute one at a time:

   ```bash
   terraform init
   terraform plan
   terraform apply
   ```





