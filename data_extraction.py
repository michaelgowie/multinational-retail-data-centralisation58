import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd
import database_utils as util
import data_cleaning as clean
import tabula
import boto3
import json




header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

class DataExtractor:
    def read_rds_table(self,dat_con_inst,table_name):
        engine = dat_con_inst.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    def retrieve_pdf_data(self, url):
        df_list = tabula.read_pdf(url,pages='all')
        card_df = df_list[0]
        for df in df_list[1:]:
            card_df = pd.concat([card_df,df], axis=0)
        return card_df
    def extract_from_s3(self, s3_address = None):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='data-handling-public',Key='products.csv')
        object_stuff = response['Body'].read().decode('utf8')
        list_of_rows = object_stuff.split('\n')
        list_of_cols = ['id']
        list_of_cols.extend(list_of_rows[0].strip().split(',')[1:])
        list_of_cols.append('dropcol')
        obj_df = pd.DataFrame([row.strip().split(',') for row in list_of_rows[1:]], columns=list_of_cols)
        obj_df.drop(['dropcol'],axis=1, inplace=True)
        return obj_df
    def extract_dates_from_s3(self):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='data-handling-public',Key='date_details.json')
        date_json_str = response['Body'].read().decode('utf8')
        date_json = json.loads(date_json_str)
        date_df = pd.DataFrame(date_json)
        return date_df




