# Databricks notebook source
# MAGIC %md
# MAGIC ####-----------------------------------------------------------------------
# MAGIC # Transform Transparencia.gov data
# MAGIC ####-----------------------------------------------------------------------
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, desc
from pyspark.sql.types import FloatType
import datetime

year = datetime.date.today().year
month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1) 
month = month.strftime('%m')

#last month because transparenciagov is yet to update their database... this month = datetime.date.today().month

# Create a SparkSession
#spark = SparkSession.builder.appName("TransformData").getOrCreate() #Remove when in databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Transform the 'receitas' data

# COMMAND ----------

# Define variables
category = 'Receitas'

# Define the path to the raw data file
raw_path = f"/mnt/adltransparenciaproject/extracted/{year}_Receitas.zip" #Adapt when in databricks

# Read the CSV file into a DataFrame (encoding = "ISO-8859-1" works)

df_receitas = spark.read.csv(raw_path, sep=r';', encoding='cp1252', header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1) Select specific columns from the DataFrame

# COMMAND ----------

df_receitas_prev = df_receitas[['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR PREVISTO ATUALIZADO']]
df_receitas_real = df_receitas[['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR REALIZADO']]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2) Replace commas with dots in value columns

# COMMAND ----------

df_receitas_prev = df_receitas_prev.withColumn("VALOR PREVISTO ATUALIZADO", regexp_replace(col("VALOR PREVISTO ATUALIZADO"), ",", "."))
df_receitas_real = df_receitas_real.withColumn("VALOR REALIZADO", regexp_replace(col("VALOR REALIZADO"), ",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.3) Cast value columns to FloatType

# COMMAND ----------

df_receitas_prev = df_receitas_prev.withColumn("valor_receita_prev", col("VALOR PREVISTO ATUALIZADO").cast(FloatType()))
df_receitas_prev = df_receitas_prev.drop("VALOR PREVISTO ATUALIZADO")

df_receitas_real = df_receitas_real.withColumn("valor_receita_real", col("VALOR REALIZADO").cast(FloatType()))
df_receitas_real = df_receitas_real.drop("VALOR REALIZADO")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.4) Extract month and year as separate columns

# COMMAND ----------

df_receitas_prev = df_receitas_prev.withColumn("date_components", split(col("DATA LANÇAMENTO"), "/"))
df_receitas_prev = df_receitas_prev.withColumn("mes", df_receitas_prev["date_components"][1].cast("int"))
df_receitas_prev = df_receitas_prev.withColumn("ano", df_receitas_prev["date_components"][2].cast("int"))

df_receitas_real = df_receitas_real.withColumn("date_components", split(col("DATA LANÇAMENTO"), "/"))
df_receitas_real = df_receitas_real.withColumn("mes", df_receitas_real["date_components"][1].cast("int"))
df_receitas_real = df_receitas_real.withColumn("ano", df_receitas_real["date_components"][2].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.5) Drop the intermediate 'date_components' column and the old column as well

# COMMAND ----------

df_receitas_prev = df_receitas_prev.drop("date_components").drop("DATA LANÇAMENTO")
df_receitas_real = df_receitas_real.drop("date_components").drop("DATA LANÇAMENTO")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.6) Rename category column

# COMMAND ----------

df_receitas_prev = df_receitas_prev.withColumnRenamed("ORIGEM RECEITA", "receita_nome")\
    .select('receita_nome', 'mes', 'ano', 'valor_receita_prev')
df_receitas_real = df_receitas_real.withColumnRenamed("ORIGEM RECEITA", "receita_nome")\
    .select('receita_nome', 'mes', 'ano', 'valor_receita_real')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.7) Send .csv to processed folder in data lake
# MAGIC

# COMMAND ----------
df_receitas_prev.na.drop(how="any")
df_receitas_real.na.drop(how="any")

df_receitas_prev.write.format("com.databricks.spark.csv").option("header","true")\
    .option("delimiter", ";").mode("overwrite").option('encoding', 'cp1252')\
        .save("/mnt/adltransparenciaproject/processed/receitas_prev")
df_receitas_real.write.format("com.databricks.spark.csv").option("header","true")\
    .option("delimiter", ";").mode("overwrite").option('encoding', 'cp1252')\
        .save("/mnt/adltransparenciaproject/processed/receitas_real")


#########################################################################
#########################################################################




# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Transform the 'orcamento-despesa' data

# COMMAND ----------

# Define variables
category = 'OrcamentoDespesa'

# Define the path to the raw data file
raw_path = f"/mnt/adltransparenciaproject/extracted/{year}_OrcamentoDespesa.zip" #Adapt when in databricks

# Read the CSV file into a DataFrame
df_orcamento_despesa = spark.read.csv(raw_path, sep=r';', encoding='cp1252', header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1) Select specific columns from the DataFrame

# COMMAND ----------

df_orcamento_despesa_ini = df_orcamento_despesa[['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO INICIAL (R$)']]
df_orcamento_despesa_atu = df_orcamento_despesa[['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO ATUALIZADO (R$)']]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2) Replace commas with dots in value columns

# COMMAND ----------

df_orcamento_despesa_ini = df_orcamento_despesa_ini.withColumn("valor_ini", regexp_replace(col("ORÇAMENTO INICIAL (R$)"), ",", "."))
df_orcamento_despesa_atu = df_orcamento_despesa_atu.withColumn('valor_atu', regexp_replace(col('ORÇAMENTO ATUALIZADO (R$)'), ",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3) Cast value columns to FloatType

# COMMAND ----------

df_orcamento_despesa_ini = df_orcamento_despesa_ini.withColumn("valor_ini", col("valor_ini").cast(FloatType()))
df_orcamento_despesa_ini= df_orcamento_despesa_ini.drop("ORÇAMENTO INICIAL (R$)")

df_orcamento_despesa_atu = df_orcamento_despesa_atu.withColumn("valor_atu", col("valor_atu").cast(FloatType()))
df_orcamento_despesa_atu = df_orcamento_despesa_atu.drop("ORÇAMENTO ATUALIZADO (R$)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4) Rename date columns

# COMMAND ----------

df_orcamento_despesa_ini = df_orcamento_despesa_ini.withColumnRenamed("NOME FUNÇÃO", "despesa_orc")\
    .withColumnRenamed("EXERCÍCIO", "ano").select("despesa_orc", 'ano', 'valor_ini')
df_orcamento_despesa_atu = df_orcamento_despesa_atu.withColumnRenamed("NOME FUNÇÃO", "despesa_orc")\
    .withColumnRenamed("EXERCÍCIO", "ano").select("despesa_orc", 'ano', 'valor_atu')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.5) Send .csv to processed folder in data lake
# MAGIC

# COMMAND ----------
df_orcamento_despesa_ini.na.drop(how="any")
df_orcamento_despesa_atu.na.drop(how="any")

df_orcamento_despesa_ini.write.format("com.databricks.spark.csv").option("header","true")\
    .option("delimiter", ";").mode("overwrite").option('encoding', 'cp1252')\
        .save("/mnt/adltransparenciaproject/processed/orcamento_despesa_ini")
df_orcamento_despesa_atu.write.format("com.databricks.spark.csv").option("header","true")\
    .option("delimiter", ";").mode("overwrite").option('encoding', 'cp1252')\
        .save("/mnt/adltransparenciaproject/processed/orcamento_despesa_atu")


#########################################################################
#########################################################################



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Transform the 'despesa-execucao' data

# COMMAND ----------

# Define variables
category = 'Despesas'

# Define the path to the raw data file
raw_path = f"/mnt/adltransparenciaproject/extracted/{year}{month}_Despesas.zip" #Adapt when in databricks

# Read the CSV file into a DataFrame
df_despesa_exec = spark.read.csv(raw_path, sep=r';', encoding='cp1252', header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1) Select specific columns from the DataFrame

# COMMAND ----------

df_despesa_exec = df_despesa_exec[['Ano e mês do lançamento', 'Nome Função','Valor Liquidado (R$)']]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2) Replace commas with dots in value columns

# COMMAND ----------

df_despesa_exec = df_despesa_exec.withColumn("valor", regexp_replace(col("Valor Liquidado (R$)"), ",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3) Cast value columns to FloatType

# COMMAND ----------

df_despesa_exec = df_despesa_exec.withColumn("valor", col("valor").cast(FloatType()))
df_despesa_exec  = df_despesa_exec.drop("Valor Liquidado (R$)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4) Extract month and year as separate columns

# COMMAND ----------

df_despesa_exec = df_despesa_exec.withColumn("date_components", split(col("Ano e mês do lançamento"), "/"))
df_despesa_exec = df_despesa_exec.withColumn("ano", df_despesa_exec["date_components"][0].cast("int"))
df_despesa_exec = df_despesa_exec.withColumn("mes", df_despesa_exec["date_components"][1].cast("int"))
df_despesa_exec = df_despesa_exec.drop("Valor Liquidado (R$)").drop("date_components")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5) Rename date columns

# COMMAND ----------

df_despesa_exec = df_despesa_exec.withColumnRenamed("NOME FUNÇÃO", "nome_despesa_exec")\
    .select("nome_despesa_exec", 'ano', 'mes', 'valor')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.6) Send .csv to processed folder in data lake
# MAGIC

# COMMAND ----------
df_despesa_exec.na.drop(how="any")

df_despesa_exec.write.format("com.databricks.spark.csv").option("header","true")\
    .option("delimiter", ";").mode("overwrite").option('encoding', 'cp1252')\
        .save("/mnt/adltransparenciaproject/processed/despesa_exec")


#########################################################################
#########################################################################

