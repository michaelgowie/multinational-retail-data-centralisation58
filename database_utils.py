import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd





class DataBaseConnector:
    '''
    This class interacts with two databases. The first is a read-only RDS database which contains the 
    data for the project. The second is a database which us created by the user to hold the retrieved data.

    Attributes:
    engine (): this is the engine which interacts with the RDS database.
    '''
    def __init__(self):
        '''
        This simply sets the engine attribute as NULL, ready to be defined later on.
        '''
        self.engine = None
    def read_db_creds(self, file_name = 'db_creds.yaml'):
        '''
        This method reads a yaml file which contains the details of the RDS database and
        stores it in a dictionary.
        '''
        with open(file_name, mode='r') as f:
            db_creds_dict = yaml.safe_load(f)
        return db_creds_dict
    def init_db_engine(self):
        '''
        This method initialises the RDS engine using the credentials provided by the read_db_creds methdod.
        '''
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
        '''
        This method returns a list of all the tables in the RDS database.
        '''
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return(inspector.get_table_names())
    def upload_to_db(self,df,table_name):
        '''
        This method uploads the a table to the created postgres database.
        It takes as input a dataframe to be uploaded and the name of the table to be applied.
        '''
        with open('postgres_password.txt', 'r') as f:
            pword = f.read()
        engine = create_engine(f"postgresql+psycopg2://postgres:{pword}@localhost:5432/sales-data")
        df.to_sql(table_name, engine, if_exists='replace')
    
    












