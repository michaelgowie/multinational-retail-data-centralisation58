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
import requests
import codecs




header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


class DataExtractor:
    '''
    This class is the one which extracts the data from various sources and puts it into 
    dataframes, ready to be cleaned.
    '''
    def read_rds_table(self,dat_con_inst,table_name):
        '''
        This retrieves data from the RDS database and puts it into a dataframe.
        As input, it takes an instance of the DataBaseConnector class and the name of the 
        table in the RDS database. It returns a DataFrame contaning the data.
        '''
        engine = dat_con_inst.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
    def retrieve_pdf_data(self, url):
        '''
        This method retrieves data from a PDF file which can be found at a URL and 
        stores it in a DataFrame.
        As input it takes a url and it outputs a DataFrame.'''
        df_list = tabula.read_pdf(url,pages='all')
        card_df = df_list[0]
        for df in df_list[1:]:
            card_df = pd.concat([card_df,df], axis=0)
        return card_df
    def extract_from_s3(self, s3_address = None):
        '''
        This method obtains a dataframe containing data which can be found at a specific
        s3 address.
        It obtains the data from the s3 address, writes it to a file and then reads it from 
        this file and puts it into a DataFrame.
        '''
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='data-handling-public',Key='products.csv')
        object_stuff = response['Body'].read().decode()
        with codecs.open('products_write.csv','w', encoding='utf8', errors='ignore') as f:
            f.write('id' + object_stuff)
        df = pd.read_csv('products_write.csv')
        return df
    def extract_dates_from_s3(self):
        '''
        This method obtains the dates data from the s3 bucket. As the data arrives in JSON
        form, the method uses json.loads to put it into a DataFrame.
        '''
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='data-handling-public',Key='date_details.json')
        date_json_str = response['Body'].read().decode()
        date_json = json.loads(date_json_str)
        date_df = pd.DataFrame(date_json)
        return date_df
    def list_number_of_stores(self, endpoint, header_dict):
        '''
        This method uses requests to obtain the number of stores from a given endpoint.
        As input, it takes the endpoint for the number of stores as well as a dictionary of headers.'''
        response = requests.get(endpoint, headers=header_dict)
        response_json = response.json()
        return response_json['number_stores']
    def retrieve_stores_data(self, endpoint, header):
        '''
        This method uses requests to obtain the data for each and every one of the stores.
        It uses the list_number_of_stors method to inform how many requests.get commands
        it should run. It stores the result of every requests.get command in a single DataFrame.'''
        num = self.list_number_of_stores(number_of_stores_endpoint, header)
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




