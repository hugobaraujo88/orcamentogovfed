# Arquitetura do pipeline: pl_ETL_ibge

![pl_ETL_ibge](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_ETL_ibge.drawio.png)

**1)** **Dataset:** *ds_sourcehttp_ibge* (dataset relativo a extração de dados no site do IBGE)

**2)** **Linked service:** *ls_http_ibge*

**3)** **Dataset:** *ds_sinkdatalake_ibge* (dataset relativo a extração dos arquivos do site da transparência em formato .zip)

**4)** **Linked service:** *ls_datalake*

**5)** **Dataset:** *ds_sourcedatalake_csv_ibge* (dataset para realizar a movimentação entre o datalake e o data flow)

**6)** **Linked service:** *ls_datalake*

**7)** **Dataset:** *ds_sinkasql_pib* (dataset relativo ao carregamento dos dados no banco de dados Azure SQL)

**8)** **Linked service:** *ls_asql*

# Pipeline no Data Factory

![pl_ETL_ibge](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_ETL_ibge.png)


