#The purpose of this code is to create schema and tables in Azure SQL or Mysql

import db_connection as dbc
from sqlalchemy import text

#Create db connection, it can be Azure SQL or MySQL
## second argument 'azure' or 'mysql'
## sbc.SQLConnection creates a SQLalchemy connection to database

db_type = 'azure'
conn = dbc.SQLConnection(f'sql_connection_{db_type}.txt',db_type).create_connection() 

#Create Historical data tables ---- #int auto_increment primary key, ---- for mysql

schema_name = 'hist'

create_sechema = text(f"""
create schema {schema_name}
"""
)

conn.execute(create_sechema)

conn.commit()

create_tb_despesa_exec = text( f"""

create table {schema_name}.despesa_exec (
    id_despesa_exec INT IDENTITY(1,1) PRIMARY KEY, 
    despesa_exec_nome varchar(255),
    ano  int,
    mes int,
    valor float
)
""" )

create_tb_orcamento_despesa_ini = text( f"""

create table {schema_name}.orcamento_despesa_ini (
    id_od_ini INT IDENTITY(1,1) PRIMARY KEY,
    despesa_orc varchar(255),
    ano  int,
    valor_ini float
)
""" )

create_tb_orcamento_despesa_atu = text( f"""

create table {schema_name}.orcamento_despesa_atu (
    id_od_atu INT IDENTITY(1,1) PRIMARY KEY,
    despesa_orc varchar(255),
    ano  int,
    valor_atu float
)
""" )

create_tb_receitas_prev = text( f"""

create table {schema_name}.receitas_prev (
    id_receitas_prev INT IDENTITY(1,1) PRIMARY KEY,
    receita_nome varchar(255),
    mes int,
    ano int,
    valor_receita_prev float
)
""" )

create_tb_receitas_real = text( f"""

create table {schema_name}.receitas_real (
    id_receitas_real INT IDENTITY(1,1) PRIMARY KEY,
    receita_nome varchar(255),
    mes int,
    ano int,
    valor_receita_real float
)
""" )

create_tb_pib = text( f"""

create table {schema_name}.pib (
    id_pib INT IDENTITY(1,1) PRIMARY KEY,
    ano int,
    valor_pib int
)
""" )

querytb_list = [create_tb_despesa_exec, create_tb_orcamento_despesa_ini,\
           create_tb_orcamento_despesa_atu, create_tb_receitas_prev,\
            create_tb_receitas_real, create_tb_pib]

for querytb in querytb_list:
    conn.execute(querytb)

conn.commit()

##################################################################################
##################################################################################
#Create dashboard data tables ---- #int auto_increment primary key, ---- for mysql
##################################################################################
##################################################################################

schema_name = 'dashboard'

create_sechema = text(f"""
create schema {schema_name}
"""
)

conn.execute(create_sechema)

conn.commit()

create_tb_previsto_exec_despesa = text( f"""

create table {schema_name}.previsto_exec_despesa (
	mes int,
	ano int,
	[Despesa Executada] varchar(255),
	[Despesa Executada Ac.] float,
	[Previsto Mensal Ac.] float,
	[Despesa Prevista Anual] float
)
""" )

create_tb_previsto_exec_receita = text( f"""

create table {schema_name}.previsto_exec_receita (
	mes int,
	ano int,
	[Receita Executada] float,
    [Receita Executada Ac.] float,
	[Previsto Mensal Ac.] float,
	[Receita Prevista Anual] float
)
""" )

create_tb_despesa_receita_exec = text( f"""

create table {schema_name}.despesa_receita_exec (
	mes int,
	ano int,
	[Despesa Executada] float,
	[Receita Executada] float
)
""" )

create_tb_princ_despesas = text( f"""

create table {schema_name}.princ_despesas (
    ano INT,
	despesa VARCHAR(255),
    valor_total_ano_despesa FLOAT,
	ranking int,
    perc_despesa_total float
)
""" )

create_tb_princ_receitas = text( f"""

create table {schema_name}.princ_receitas (
	ano INT,
	receita VARCHAR(255),
	valor_total_ano_receita FLOAT,
	ranking int,
	perc_receita_total float
)
""" )

create_tb_despesa_exec_ac_pib = text( f"""

create table {schema_name}.despesa_exec_ac_pib
(
	ano int,
	[Despesa Exec. % PIB] float
)
""" )


create_tb_receita_arrec_ac_pib = text( f"""

create table {schema_name}.receita_arrec_ac_pib
(
	ano int,
	[Receita Arrec. % PIB] float
)
""" )

querytb_list = [create_tb_previsto_exec_despesa, create_tb_previsto_exec_receita, \
                create_tb_despesa_receita_exec, create_tb_princ_despesas, \
                    create_tb_princ_receitas,\
                        create_tb_despesa_exec_ac_pib, create_tb_receita_arrec_ac_pib]


for querytb in querytb_list:
    conn.execute(querytb)

conn.commit()

conn.close()