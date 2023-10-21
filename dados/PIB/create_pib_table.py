# Downloading Brazilian GDP data (PIB)


#Importing GDP table from IBGE Sidra https://apisidra.ibge.gov.br/
#getting only value 'V' during the quarter 'D2C'

import sidrapy as spy

pib = spy.get_table(
        table_code = '1846', 
        territorial_level = '1', 
        ibge_territorial_code = 'all', 
        classification = '11255/90707', 
        period = 'all' )[['V','D2C']]

#Change the column names to those in row 0

pib.columns = pib.iloc[0]

pib = pib.drop(0)

#Transforming GDP quarterly data into yearly data

pib['Valor'] = pib['Valor'].astype(int)

pib['Ano'] = pib['Trimestre (Código)'].str[:4]

pib = pib.groupby('Ano')['Valor'].sum().reset_index()

# write file to a csv

folder_path = 'transparencia\\Dados\\PIB'

file_name = '_pib.csv'

pib.to_csv(f'{folder_path}/{file_name}', index=False)


