# Projeto: Orçamento do Governo Federal | Panorama

Este projeto tem como objetivo extrair e analisar dados do orçamento federal, apresentando-os em um formato acessível e informativo por meio de um dashboard interativo. **O objetivo é permitir uma análise rápida e integrada, facilitando o entendimento de como os recursos do governo federal são arrecadados e alocados.**

## Arquitetura do projeto (visão geral)

![Exemplo de Imagem](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/transparencia_data_arch.png)

**1)** Dados são extraidos do Portal da Transparência da CGU (Controladoria Geral da União): Despesas Executadas, Orçamento das Despesas (seria o planejado), Receitas Previstas e Arrecadadas.

**2)** Dados são extraídos do site do IBGE: PIB trimestral.

**3)** Dados extraídos são armazenados em Data Lake do Azure.

**4)** Os dados brutos extraídos do Portal da Transparência são transformados utilizado o PySpark no ambiente do Data Bricks: modificação do nome das colunas, remoção de colunas desnecessárias, criando colunas numéricas (float, e int) e alguns "splits" nos dados brutos.

**5)** Dados brutos extraídos do IBGE são transformados utilizando o Data Flow (que também usa um cluster Spark): basicamente converte o PIB trimestral em anual.

**6)** Dados processados são carregados em um banco de dados Azure SQL. (o schema e a estrutura das tabelas foram criados por meio deste código: [create_SQL_schema.py](https://github.com/hugobaraujo88/orcamentogovfed/blob/main/create_SQL_schema.py) )

**7)** Dados históricos, de 2014 a 2023, são extraídos, processados e enviados ao Azure SQL por meio de um código em python (ver [download_orcamento_federal.py](https://github.com/hugobaraujo88/orcamentogovfed/blob/main/download_orcamento_federal.py) e [send_historical_to_sql.py](https://github.com/hugobaraujo88/orcamentogovfed/blob/main/send_historical_to_sql.py))

**8)** Por fim, após a realização das queries contidas na pasta [SQL Scripts](https://github.com/hugobaraujo88/orcamentogovfed/tree/main/SQL%20Scripts), queries estas que são executadas automaticamente cada vez que o pipeline de carregamento é acionado, o dashboard é criado no Power BI via direct query.

Neste link: https://youtu.be/dawEcPuuV1s, é possível ter uma descrição resumida do projeto e da arquitetura **além da gravação, em tempo real, dos pipelines do projeto em execução**.

## Pré-requisitos para execução desse projeto

**-** Python + libs (requests, Pandas, textwrap, pyodbc, sqlalchemy, zipfile, os, urllib.parse ) 

**-** Leitor de arquivos .ipynb (análise exploratória dos dados [analise_exploratoria.ipynb](https://github.com/hugobaraujo88/orcamentogovfed/blob/main/analise_exploratoria.ipynb))

**-** Conta Azure

## Serviços utilizados na nuvem

A tabela a seguir apresenta todos os serviços azure utilizados para realizar o projeto. Na pasta [Templates - Azure](https://github.com/hugobaraujo88/orcamentogovfed/tree/main/Templates%20-%20Azure), é possível encontrar o todos os 

| Serviço       | Descrição     |
| ------------- | ------------- |
| Microsoft Entra  | Content Cell  |
| Content Cell  | Content Cell  |