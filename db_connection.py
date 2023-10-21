##Class to create a db connection (AzureSQL or MySQL)

import pyodbc
from sqlalchemy import create_engine
import urllib.parse

class SQLConnection:
    #Config Data File path as an input to create de connection object
    
    def __init__(self, config_file_path, db_type): 
        self.config_file_path = config_file_path
        self.db_type = db_type

    def create_connection(self):
        if self.db_type == 'azure':
            return self._create_azure_connection()
        elif self.db_type == 'mysql':
            return self._create_mysql_connection()
        elif self.db_type == 'mssql':
            return self._create_mssql_connection()
        else:
            raise ValueError("Unsupported database type")

    def _read_config_file(self):
        
        config_data = {}
        
        # Open the file in read mode
        with open(self.config_file_path, 'r') as file:
        # Read each line in the file     
            for line in file:
        # Split each line into key and value        
                key, value = map(str.strip, line.strip().split('='))
        # Store the key-value pair in the dictionary
                config_data[key] = value
        return config_data
    
    # Connect to azure SQL Database (important to set the server firewall 
    # at Azure portal for the IP address)
    def _create_azure_connection(self):

        config_data = self._read_config_file()
        driver = config_data.get('driver')
        server = config_data.get('server')
        database = config_data.get('database')
        username = config_data.get('username')
        password = config_data.get('password')
        
        params = urllib.parse.quote_plus(
            'Driver=%s;' % driver +
            'Server=tcp:%s,1433;' % server +
            'Database=%s;' % database +
            'Uid=%s;' % username +
            'Pwd={%s};' % password +
            'Encrypt=yes;' +
            'TrustServerCertificate=no;' +
            'Connection Timeout=30;')

        conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
        engine = create_engine(conn_str)

        return engine.connect()
    
    # Connect to MySQL SQL Database
    def _create_mysql_connection(self):
        config_data = self._read_config_file()
        username = config_data.get('username')
        password = config_data.get('password')
        
        conn_string = f'mysql+pymysql://{username}:{password}@localhost'
        engine = create_engine(conn_string)

        return engine.connect()
    
    #Connect to MSSQL database (windows authentication, no password needed...)
    def _create_mssql_connection(self):
        config_data = self._read_config_file()
        server = config_data.get('server')
        database = config_data.get('database')
        
        conn_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"
        engine = pyodbc.connect(conn_string)

        return engine.cursor()
