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

2. You need an AWS security credential to store the csv files. Access your ```AWS console``` and search for ```IAM > Access keys```
   Save ```AWS_ACCESS_KEY_ID```, ```AWS_SECRET_ACCESS_KEY```, be it what you created now or what you already have.
   
3. After that you need to run a docker container. Make sure docker is running. This is an important step because this is where the AWS environment variables will be passed. 
   It needs to be in this folder because a docker volume will be created so that it is also possible to work with local files.

   Below you can see that some changes need to be made.

   Ensure that ```-v /Users/mauriciobueno/iff:/home/infra``` is your cloned folder, i.e. ```-v 'YourPathHere'/iff:/home/infra```
   Make sure you have the correct name for your bucket, in this case I chose ifftest1. Remembering that the name must be unique.
    
   ```bash
   docker run -d --name infra_iff -p 8080:8080 \
     -v <your path here>/iff:/home/infra \
     -e AWS_ACCESS_KEY_ID=<your access key here> \
     -e AWS_SECRET_ACCESS_KEY=<your secret key here> \
     -e AWS_DEFAULT_REGION=sa-east-1 \
     -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
     -e BUCKET_NAME=<your bucket with unique name> \
     infra_iff
   ```


## Use
After that, the container should be running and the folder should have the following characteristics.

<img width="444" alt="image" src="https://github.com/user-attachments/assets/bac4cc8e-2b6c-45f2-9570-75fe7555a166" />



   1 - With the local terminal in the /iff folder, you now need to access the container terminal.

   ```bash   
       docker exec -it infra_iff bash
   ```

<img width="867" alt="image" src="https://github.com/user-attachments/assets/b1c80a71-48b4-4aa8-a5e3-9af47888464a" />


From now on, the development environment is already working, if the bucket already exists! 🎉🎉 

---


⚠️ If the bucket does not exist on AWS. Follow the procedure below to create it via Terraform. ⚠️

Access the ```/iff``` folder and then modify the file main.ts and save. Define the name of your new bucket to be created, ensuring that it is unique in AWS. 


⚠️ This name must be the same defined above ```-e BUCKET_NAME=<your bucket with unique name>```  
      

<img width="884" alt="image" src="https://github.com/user-attachments/assets/ed6a662c-b386-425e-a6b8-40aa6c151266" />

   
   2- Execute one at a time
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

Now you can see that the bucket was created successfully.

<img width="1631" alt="image" src="https://github.com/user-attachments/assets/d2ea07b7-e3a6-4222-a52a-4c83ede163d4" />

---

## Uso

Now everything is configured. airflow is already running locally at http://localhost:8080

<img width="1680" alt="image" src="https://github.com/user-attachments/assets/04d81c08-a7a3-43ab-9b84-f91714b65bbc" />







