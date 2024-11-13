import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd
import requests



header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

class DataBaseConnector:
    def __init__(self):
        self.engine = None
    def read_db_creds(self):
        with open('db_creds.yaml',mode='r') as f:
            db_creds_dict = yaml.safe_load(f)
        return db_creds_dict
    def init_db_engine(self):
        creds_dict = self.read_db_creds()
        HOST = creds_dict['RDS_HOST']
        PASSWORD = creds_dict['RDS_PASSWORD']
        USER = creds_dict['RDS_USER']
        DATABASE = creds_dict['RDS_DATABASE']
        PORT = creds_dict['RDS_PORT']
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        self.engine = engine
        return engine
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return(inspector.get_table_names())
    def upload_to_db(self,df,table_name):
        engine = create_engine(f"postgresql+psycopg2://postgres:987621@localhost:5432/sales-data")
        df.to_sql(table_name, engine, if_exists='replace')
    def list_number_of_stores(self, endpoint, header_dict):
        response = requests.get(endpoint, headers=header_dict)
        response_json = response.json()
        return response_json['number_stores']
    def retrieve_stores_data(self, endpoint, header):
        num = self.list_number_of_stores(num_endpoint, header)
        endpoint_zero = endpoint + '0'
        first_data = requests.get(endpoint_zero,headers=header).json()
        stores_df = pd.DataFrame(first_data,index=[0])
        for i in range(1,num):
            total_endpoint = endpoint + str(i)
            response = requests.get(total_endpoint,headers=header)
            response_json = response.json()
            df_i = pd.DataFrame(response_json, index=[0])
            stores_df = pd.concat([stores_df,df_i],ignore_index=True)
            df_i = None
        return stores_df
    






num_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'






