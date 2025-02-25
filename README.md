# IFF Project

To meet the proposed objective, we implemented a solution based on the Medallion Architecture, processing 9 sets of synthetic data that represent different business entities, such as customers, suppliers, revenues, among others. The system was designed to ingest, transform and model this data into three distinct tiers — Bronze, Silver and Gold — using modern, open source tools. Below, detail how the objective was achieved using Terraform, Airflow, Docker and DuckDB.

Docker is required as the virtual infrastructure is established there. This project was carried out in a MacOS environment and virtualized in Linux using container.

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

### Steps

Follow these instructions to install the project locally:

1. Clone the repository:
   ```bash
   git clone https://github.com/seu-usuario/seu-repositorio.git


