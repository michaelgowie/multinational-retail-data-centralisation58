import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd



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
    
    












