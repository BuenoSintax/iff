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
   cd <your path here>
   git clone https://github.com/BuenoSintax/iff.git
   ```

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
   Here you will define the name of your AWS bucket. It needs to be globally ```unique```, in my case I chose ifftest1. If you don't have it, choose it anyway, because more
   below I'll show you how to do it with Terraform.
    
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
### Creating bucket

⚠️ If the bucket does not exist on AWS, follow the procedure below to create it via Terraform. ⚠️

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



Now everything is configured and airflow is already running locally at http://localhost:8080

<img width="1680" alt="image" src="https://github.com/user-attachments/assets/04d81c08-a7a3-43ab-9b84-f91714b65bbc" />

---
## Diagram

### Class Overview

Below is a brief overview of each class about the project.

### **dp_salestransactions_silver**
- **Purpose**: Represents sales transactions.
- **Key Fields**:
  - **transaction_id** (PK): Unique identifier of the transaction.
  - **customer_id** (FK): Links to the customer who made the purchase.
  - **flavour_id** (FK): References the flavor involved in the transaction.
  - **quantity_liters**: Amount of product in liters.
  - **transaction_date**: Date when the transaction occurred.
  - **country**, **town**, **postal_code**: Geographic details.
  - **amount_dollar**: Transaction amount in US dollars.
  - **load_timestamp**, **source_date**: Metadata for data loading and lineage.

--

### **dp_customers_silver**
- **Purpose**: Contains customer data.
- **Key Fields**:
  - **customer_id** (PK): Unique identifier of the customer.
  - **name**, **city**, **country**: Core customer details.
  - **valid_from**, **valid_to**, **active**: Tracks the lifecycle and status of the customer record.
  - **load_timestamp**, **source_date**: Data processing timestamps.

--

### **dp_flavours_silver**
- **Purpose**: Holds information about flavors.
- **Key Fields**:
  - **flavour_id** (PK): Unique identifier of the flavor.
  - **name**, **description**: Flavor name and details.
  - **valid_from**, **valid_to**, **active**: Controls the lifecycle of the flavor record.
  - **load_timestamp**, **source_date**: Data audit timestamps.

--

### **dp_recipes_silver**
- **Purpose**: Defines recipe compositions.
- **Key Fields**:
  - **recipe_id** (PK): Unique identifier of the recipe.
  - **flavour_id** (FK): Links to the associated flavor.
  - **ingredient_id** (FK): Links to the required ingredient.
  - **quantity_grams**: Amount of ingredient in grams.
  - **heat_process**: Describes any specific heat treatment in the recipe.
  - **valid_from**, **valid_to**, **active**: Manages recipe versioning and availability.
  - **load_timestamp**, **source_date**: Data lineage timestamps.

--

### **dp_ingredients_silver**
- **Purpose**: Stores ingredient details.
- **Key Fields**:
  - **ingredient_id** (PK): Unique identifier of the ingredient.
  - **name**, **chemical_formula**, **molecular_weight**: Main properties.
  - **cost_per_gram**: Pricing attribute.
  - **provider_id** (FK): References the supplier (provider).
  - **valid_from**, **valid_to**, **active**: Lifecycle management of the ingredient.
  - **load_timestamp**, **source_date**: Timestamps for data updating.

--

### **dp_provider_silver**
- **Purpose**: Contains provider (supplier) information.
- **Key Fields**:
  - **provider_id** (PK): Unique identifier of the provider.
  - **name**, **city**, **country**: Basic provider details.
  - **valid_from**, **valid_to**, **active**: Lifecycle fields to track provider status.
  - **load_timestamp**, **source_date**: Audit timestamps.

--

### **dp_ingredientsrawmaterial_silver**
- **Purpose**: Maps ingredients to their raw material types.
- **Key Fields**:
  - **ingredient_rawmaterial_id** (PK): Unique identifier of the mapping record.
  - **ingredient_id** (FK): References the ingredient.
  - **raw_material_type_id** (FK): References the raw material type.
  - **valid_from**, **valid_to**, **active**: Tracks the validity period for the mapping.
  - **load_timestamp**, **source_date**: Audit timestamps.

--

### **dp_rawmaterialtype_silver**
- **Purpose**: Describes categories or types of raw materials.
- **Key Fields**:
  - **raw_material_type_id** (PK): Unique identifier of the raw material type.
  - **name**: Name of the raw material category.
  - **valid_from**, **valid_to**, **active**: Controls the lifecycle of each raw material type.
  - **load_timestamp**, **source_date**: Data audit timestamps.

###Above a diagram class

```mermaid
---
title: Diagram class
---
classDiagram
    direction LR
    class dp_salestransactions_silver {
        -transaction_id : INTEGER PK
        -customer_id : INTEGER FK
        -flavour_id : INTEGER FK
        -quantity_liters : DOUBLE
        -transaction_date : DATE
        -country : VARCHAR
        -town : VARCHAR
        -postal_code : VARCHAR
        -amount_dollar : DOUBLE
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_customers_silver {
        -customer_id : INTEGER PK
        -name : VARCHAR
        -city : VARCHAR
        -country : VARCHAR
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_flavours_silver {
        -flavour_id : INTEGER PK
        -name : VARCHAR
        -description : VARCHAR
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_recipes_silver {
        -recipe_id : INTEGER PK
        -flavour_id : INTEGER FK
        -ingredient_id : INTEGER FK
        -quantity_grams : DOUBLE
        -heat_process : VARCHAR
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_ingredients_silver {
        -ingredient_id : INTEGER PK
        -name : VARCHAR
        -chemical_formula : VARCHAR
        -molecular_weight : DOUBLE
        -cost_per_gram : DOUBLE
        -provider_id : INTEGER FK
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_provider_silver {
        -provider_id : INTEGER PK
        -name : VARCHAR
        -city : VARCHAR
        -country : VARCHAR
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_ingredientsrawmaterial_silver {
        -ingredient_rawmaterial_id : INTEGER PK
        -ingredient_id : INTEGER FK
        -raw_material_type_id : INTEGER FK
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    class dp_rawmaterialtype_silver {
        -raw_material_type_id : INTEGER PK
        -name : VARCHAR
        -valid_from : TIMESTAMP
        -valid_to : TIMESTAMP
        -active : BOOLEAN
        -load_timestamp : TIMESTAMP
        -source_date : DATE
    }

    dp_salestransactions_silver "1" --o "1" dp_customers_silver : customer_id
    dp_salestransactions_silver "1" --o "1" dp_flavours_silver : flavour_id
    dp_flavours_silver "1" --o "*" dp_recipes_silver : flavour_id
    dp_recipes_silver "1" --o "1" dp_ingredients_silver : ingredient_id
    dp_ingredients_silver "1" --o "1" dp_provider_silver : provider_id
    dp_ingredients_silver "1" --o "*" dp_ingredientsrawmaterial_silver : ingredient_id
    dp_ingredientsrawmaterial_silver "1" --o "1" dp_rawmaterialtype_silver : raw_material_type_id
```
