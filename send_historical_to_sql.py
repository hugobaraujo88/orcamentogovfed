#Send dataframes to SQL database

import data_prep as dp
import db_connection as dbc
import pandas as pd

#Create connection with SQL database
db_type = 'azure'
conn = dbc.SQLConnection(f'sql_connection_{db_type}.txt', db_type).create_connection() 

## Create objects to perfrom the csv reading
despesa_exec = dp.CSVReader('dados/despesas-execucao')
orcamento_despesa = dp.CSVReader('dados/orcamento-despesa')
rceitas = dp.CSVReader('dados/receitas')
pib = dp.CSVReader('dados/pib')


years_months = [year * 100 + month for year in range(2014, 2024) for month in range(1, 13)]

years = [year for year in range(2014, 2024)]

#########################################################################
## Send despeas_exec dataframe to SQL Table hist.despesa_exec ##
#########################################################################

for yearmonth in years_months:
    try:
        df_despesa_exec= despesa_exec.read_file(yearmonth,'Despesas')

        # Prepare the data before sending to SQL
        # Delete unecessary columns

        selected_columns = ['Ano e mês do lançamento', 'Nome Função','Valor Liquidado (R$)']

        df_despesa_exec = df_despesa_exec[selected_columns]

        #Other Transformations

        transformation_applied = dp.DataTransformation(df_despesa_exec)

        #Column String to float

        transformation_applied.replace_characters('Valor Liquidado (R$)', ',', '.')

        transformation_applied.convert_to_numeric('Valor Liquidado (R$)')

        #Split column 'Ano e mês do lançamento' in 2 columns 'ano' e 'mes'

        transformation_applied.split_column('Ano e mês do lançamento', r'/', ['ano', 'mes'])

        transformation_applied.convert_to_numeric(['ano','mes'])

        #renaming columns, according to the SQL Table

        old_column_names = ['Nome Função','Valor Liquidado (R$)']
        column_renames = ['despesa_exec_nome', 'valor']

        transformation_applied.rename_columns(old_column_names, column_renames)

        df_despesa_exec = df_despesa_exec.dropna(how='any')

        #Send Dataframe To a SQL Table

        try:
            df_despesa_exec.to_sql(name = 'despesa_exec', schema ='hist',\
                                if_exists = 'append', con = conn, index = False)
        except FileNotFoundError:
            print(f"Cannot send {yearmonth}_Despesas.csv to SQL.")
    except:
        continue

###################################################################################################
## Send df_orcamento_despesa_ini/df_orcamento_despesa_atu dataframes to corresponding SQL Tables ##
###################################################################################################
 
for year in years:

    df_orcamento_despesa = orcamento_despesa.read_file(year,'OrcamentoDespesa.zip')

    #Divide dataframe into 'ini' and 'atu', exclude unnecessary columns

    df_orcamento_despesa_ini = df_orcamento_despesa[['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO INICIAL (R$)']]
    df_orcamento_despesa_atu = df_orcamento_despesa[['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO ATUALIZADO (R$)']]

    transformation_applied_ini = dp.DataTransformation(df_orcamento_despesa_ini)
    transformation_applied_atu = dp.DataTransformation(df_orcamento_despesa_atu)

    #Transform string to numeric

    transformation_applied_ini.replace_characters('ORÇAMENTO INICIAL (R$)', ',', '.')
    transformation_applied_atu.replace_characters('ORÇAMENTO ATUALIZADO (R$)', ',', '.')

    transformation_applied_ini.convert_to_numeric(['ORÇAMENTO INICIAL (R$)', 'EXERCÍCIO'])
    transformation_applied_atu.convert_to_numeric(['ORÇAMENTO ATUALIZADO (R$)', 'EXERCÍCIO'])

    #Rename columns to a more code friendly names

    old_column_names_ini = ['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO INICIAL (R$)']
    column_renames_ini = ['ano', 'despesa_orc','valor_ini']

    old_column_names_atu = ['EXERCÍCIO', 'NOME FUNÇÃO','ORÇAMENTO ATUALIZADO (R$)']
    column_renames_atu = ['ano', 'despesa_orc','valor_atu']

    transformation_applied_ini.rename_columns(old_column_names_ini, column_renames_ini)
    transformation_applied_atu.rename_columns(old_column_names_atu, column_renames_atu)

    df_orcamento_despesa_ini = df_orcamento_despesa_ini.dropna(how='any')
    df_orcamento_despesa_atu= df_orcamento_despesa_atu.dropna(how='any')
                                
    #Send Dataframe To a SQL Table

    try:
        df_orcamento_despesa_ini.to_sql(name = 'orcamento_despesa_ini', schema ='hist',\
                            if_exists = 'append', con = conn, index = False)
    except FileNotFoundError:
        print(f"Cannot send {year}_orcamento_despesa_ini.csv to SQL.")
    
    try:
        df_orcamento_despesa_atu.to_sql(name = 'orcamento_despesa_atu', schema ='hist',\
                            if_exists = 'append', con = conn, index = False)
    except FileNotFoundError:
        print(f"Cannot send {year}_orcamento_despesa_atu.csv to SQL.")

###################################################################################
## Send df_receitas_prev/df_receitas_real dataframes to corresponding SQL Tables ##
###################################################################################

for year in years:

    df_receitas = rceitas.read_file(year,'Receitas')

    #Divide dataframe into 'prev' and 'real', exclude unnecessary columns

    df_receitas_prev = df_receitas[['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR PREVISTO ATUALIZADO']]
    df_receitas_real = df_receitas[['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR REALIZADO']]

    transformation_applied_prev = dp.DataTransformation(df_receitas_prev)
    transformation_applied_real = dp.DataTransformation(df_receitas_real)

    #Transform string to numeric

    transformation_applied_prev.replace_characters('VALOR PREVISTO ATUALIZADO', ',', '.')
    transformation_applied_real.replace_characters('VALOR REALIZADO', ',', '.')

    transformation_applied_prev.convert_to_numeric('VALOR PREVISTO ATUALIZADO')
    transformation_applied_real.convert_to_numeric('VALOR REALIZADO')

    #Rename columns to a more code friendly names

    old_column_names_prev = ['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR PREVISTO ATUALIZADO']
    column_renames_prev = ['data', 'receita_nome','valor_receita_prev']

    old_column_names_real = ['DATA LANÇAMENTO', 'ORIGEM RECEITA','VALOR REALIZADO']
    column_renames_real = ['data', 'receita_nome','valor_receita_real']
    
    transformation_applied_prev.rename_columns(old_column_names_prev, column_renames_prev)
    transformation_applied_real.rename_columns(old_column_names_real, column_renames_real)
    
    #Create year (ano) and month (mes) columns

    transformation_applied_prev.split_column('data', r'/', ['dia','mes', 'ano'])
    transformation_applied_real.split_column('data', r'/', ['dia','mes', 'ano'])

    transformation_applied_prev.convert_to_numeric(['mes', 'ano'])
    transformation_applied_real.convert_to_numeric(['mes', 'ano'])

    df_receitas_prev = df_receitas_prev[['receita_nome', 'mes', 'ano', 'valor_receita_prev']]
    df_receitas_real = df_receitas_real[['receita_nome', 'mes', 'ano', 'valor_receita_real']]

    df_receitas_prev = df_receitas_prev.dropna(how='any')
    df_receitas_real = df_receitas_real.dropna(how='any')

    #Send Dataframe To a SQL Table

    try:
        df_receitas_prev.to_sql(name = 'receitas_prev', schema ='hist',\
                            if_exists = 'append', con = conn, index = False)
    except FileNotFoundError:
        print(f"Cannot send {year}_receitas_prev.csv to SQL.")
    
    try:
        df_receitas_real.to_sql(name = 'receitas_real', schema ='hist',\
                            if_exists = 'append', con = conn, index = False)
    except FileNotFoundError:
        print(f"Cannot send {year}_receitas_real.csv to SQL.")

########################################################
## Send /df_pib dataframes to corresponding SQL Table ##
########################################################

df_pib = pib.read_file('','pib')

transformation_applied_pib = dp.DataTransformation(df_pib)

transformation_applied_pib.split_column('Ano,Valor', r',', ['ano', 'valor_pib'])

transformation_applied_pib.convert_to_numeric('ano')

transformation_applied_pib.convert_to_numeric('valor_pib')

try:
    df_pib.to_sql(name = 'pib', schema ='hist',\
                        if_exists = 'append', con = conn, index = False)
except FileNotFoundError:
    print(f"Cannot send _pib.csv to SQL.")

conn.close()