# Arquitetura do pipeline: pl_extract_transparenciagov

![pl_extract_transparenciagov](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_extract_transparenciagov.drawio.png)

**1)** **Dataset:** *ds_sourcedatalake_json_lookup* (dataset para ler o arquivo: readurls.json)

**2)** **Linked service:** *ls_datalake*

**3)** **Dataset:** *ds_sourcehttp_transparenciagov* (dataset relativo a extração dos arquivos do site da transparência em formato .zip)

**4)** **Linked service:** *ls_http_transparenciagov*

**5)** **Dataset:** *ds_sinkdatalake_zip_transparenciagov* (dataset relativo ao armazenamento dos aqruivos .zip no data lake)

**6)** **Linked service:** *ls_datalake*

**7)** **Dataset:** *ds_sourcedatalake_zip_transparenciagov* (dataset relativo a extração dos .csv's, a partir dos aqruivos .zip)

**8)** **Linked service:** *ls_datalake*

**9)** **Dataset:** *ds_sinkdatalake_csv_transparenciagov* (dataset relativo ao carregamento do data lake com os arquivos .csv originados do portal da transparência)

**10)** **Linked service:** *ls_datalake*

# Pipeline no Data Factory

![pl_extract_transparenciagov1](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_extract_transparenciagov1.png)


![pl_extract_transparenciagov2_foreach](https://raw.githubusercontent.com/hugobaraujo88/orcamentogovfed/main/img/pl_extract_transparenciagov2_foreach.png)
