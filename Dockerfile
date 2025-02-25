# Use Python 3.9 slim como imagem base
FROM python:3.9-slim

# ------------------------------------------------------------
# Variáveis de ambiente
# ------------------------------------------------------------
ENV AIRFLOW_HOME=/home/infra/airflow
ENV AIRFLOW_VERSION=2.5.1
ENV PYTHON_VERSION=3.9
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Versão do Terraform
ARG TF_VERSION=1.4.2
# Versão do DuckDB CLI a instalar
ARG DUCKDB_VERSION=0.7.1

# ------------------------------------------------------------
# ETAPA 1: Instala pacotes do sistema e Node.js
# ------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-ainstall-recommends \
    curl \
    wget \
    unzip \
    libpq-dev \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    gnupg2 \
    lsb-release \
    ca-certificates \
    awscli \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instala Node.js 18 via NodeSource
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# ETAPA 2: Instala o Terraform
# ------------------------------------------------------------
RUN wget https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip \
    && unzip terraform_${TF_VERSION}_linux_amd64.zip \
    && mv terraform /usr/local/bin/terraform \
    && rm terraform_${TF_VERSION}_linux_amd64.zip

# ------------------------------------------------------------
# ETAPA 3: Instala a CLI do DuckDB
# ------------------------------------------------------------
RUN wget -q https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /usr/local/bin/duckdb \
    && rm duckdb_cli-linux-amd64.zip

# ------------------------------------------------------------
# ETAPA 4: Instala Airflow + Providers + duckdb (lib Python)
# ------------------------------------------------------------
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    boto3 \
    duckdb

# ------------------------------------------------------------
# ETAPA 5: Cria diretórios, arquivo de entrypoint e usuário "infra"
# ------------------------------------------------------------
# Cria o diretório que será usado como volume para persistência

RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'mkdir -p /home/infra/airflow/dags' >> /entrypoint.sh && \
    echo 'airflow db init' >> /entrypoint.sh && \
    echo 'airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com' >> /entrypoint.sh && \
    echo 'airflow standalone' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh



# Cria o usuário "infra" e atribui a propriedade do diretório /home/infra
RUN useradd -ms /bin/bash infra && chown -R infra:infra /home/infra

# Cria a pasta para as DAGs
RUN mkdir -p /home/infra/airflow/dags

# ------------------------------------------------------------
# ETAPA 6: Define o diretório de volume, usuário, workdir e comando de entrada
# ------------------------------------------------------------
# Define /home/infra como volume para ser mapeado com o diretório do host
# Nota: Para persistir as DAGs, monte um volume do host para /home/infra/airflow ao executar o container
VOLUME ["/home/infra"]

USER infra
WORKDIR /home/infra
CMD ["/entrypoint.sh"]