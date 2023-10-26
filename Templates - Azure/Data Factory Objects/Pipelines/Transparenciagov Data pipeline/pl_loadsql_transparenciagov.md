# Arquitetura do pipeline: pl_load_transparenciagov

![pl_extract_transparenciagov](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_loadsql_transparenciagov.drawio.png)

**1)** **Dataset:** *ds_sourcedatalake_getfoldernames* (dataset da atividade "get metadata" que basicamente lê os nomes das pastas que foram criadas após o processamento via pySpark)

**2)** **Linked service:** *ls_datalake*

**3)** **Dataset:** *ds_sourcedatalake_csv_processed* (dataset relativo a extração dos dados no data lake em direção ao Azure SQL)

**4)** **Linked service:** *ls_datalake*

**5)** **Dataset:** *ds_sinkasql_transparenciagov* (dataset para o carregamento dos dados no Azure SQL)

**6)** **Linked service:** *ls_asql*

# Pipeline no Data Factory

![pl_loadsql_transparenciagov](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_loadsql_transparenciagov.png)


![pl_loadsql_transparenciagov_foreach](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_loadsql_transparenciagov_foreach.png)