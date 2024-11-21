import pandas as pd
import numpy as np
import re
from dateutil.parser import parse

class DataCleaning:
    '''
    This class contains functions which clean the various dataframes created by the DataExctractor class.
    '''
    def clean_user_data(self,df):
        '''
        This function takes in the user DataFrame created by DataExtractor.read_rds_table method and cleans it. 
        It creates a dataframe with no NULL values or "NULL" strings and one in which the country code is always one of the three
        possible. Further, the clean DataFrame has a join_date column which is of a date data type.
        '''
        df_rep = df
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan,'GGB':'GB'})
        country_code_mask = df_rep.country_code.isin(['GB','DE','US','GGB'])
        df_rep = df_rep[country_code_mask]
        df_rep = df_rep.dropna(axis=0,how='any')
        df_rep.join_date = df_rep.join_date.apply(parse)
        df_rep.join_date = pd.to_datetime(df_rep.join_date, errors='coerce')
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_card_data(self,df):
        '''
        This function takes in a dataframe obtained by the DataExtractor.retrieve_pdf_data method and returns a clean
        version.
        It removes rows with NULL values and ones in which the card_number value contains letters.
        '''
        df_rep = pd.DataFrame()
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan, })
        df_rep = df_rep.dropna(axis=0,how='any')
        card_num_mask = ~df_rep['card_number'].astype(str).str.isupper()
        df_rep = df_rep[card_num_mask]
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_store_data(self, df):
        '''
        This function takes in a DataFrame created by the DataExtractor.retrieve_stores_data
        method and returns a clean version.
        It removes all rows where the country_code is not one of the three possibilities and ensures that the staff_numbers
        column contains only digits.'''
        country_code_mask = df['country_code'].isin(['DE','GB','US'])
        df = df[country_code_mask]
        df.staff_numbers = df.staff_numbers.str.strip()
        df.staff_numbers = df.staff_numbers.replace({r'[^0-9]+':''},regex=True)
        return df
    def convert_product_weights(self,df):
        '''
        This method converts the product weights into floats.
        The DataFrame obtained by the DataExtractor.extract_from_s3 method has a weight column
        in which the weights are given as a string and in different units of measurement.
        This method changes all of them to a float in the KG units.'''
        def convert_weight(weight_str):
            '''
            This function takes in a string and if it is of the form xxxkg, xxxg or xxxml, it returns a weight as a 
            float in KG. Otherwise, it returns np.nan.'''
            weight_str = str(weight_str)
            if weight_str.endswith('kg'):
                if 'x' in weight_str:
                    weight_str_split = weight_str.split(' ')
                    try:
                        total_wt = float(weight_str_split[0]) * float(weight_str_split[2][:-2])
                        return total_wt
                    except:
                        return np.nan
                try:
                    wt_float = float(weight_str[:-2])
                    return wt_float
                except:
                    return np.nan
            elif weight_str.endswith('g'):
                if 'x' in weight_str:
                    weight_str_split = weight_str.split(' ')
                    try:
                        total_wt = float(weight_str_split[0]) * float(weight_str_split[2][:-1])
                        return total_wt
                    except:
                        return np.nan
                try:
                    wt_float = float(weight_str[:-1])
                    return wt_float / 1000
                except:
                    return np.nan
            elif weight_str.endswith('ml'):
                if 'x' in weight_str:
                    weight_str_split = weight_str.split(' ')
                    try:
                        total_wt = float(weight_str_split[0]) * float(weight_str_split[2][:-1])
                        return total_wt
                    except:
                        return np.nan
                try:
                    wt_float = float(weight_str[:-2])
                    return wt_float / 1000
                except:
                    return np.nan
            else:
                return np.nan
        df['weight'] = df['weight'].map(convert_weight)
        return df
    def clean_product_data(self,df):
        '''
        This method cleans the DataFrame obtained by the DataExtractor.extract_from_s3 method.
        It uses the convert_product_weights method and removes NULL values from the DataFrame.'''
        df_rep = pd.DataFrame()
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan})
        for column in list(df):
            mask = df_rep[column].isna() 
        df_rep._set_value(1779,'weight',0.077)
        df_rep._set_value(1841,'weight',0.454)
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_orders_data(self,df):
        '''
        This method removes columns from the orders DataFrame obtained by the DataExtractor.read_rds_table method
        to preserve the anonimity of the orders.'''
        df.drop(['1','first_name','last_name','level_0'],axis=1, inplace=True)
        return df
    def clean_dates_data(self,df):
        '''
        This method cleans the DataFrame obtained by the DataExtractor.extract_dates_from_s3 method.
        It removes Null values and also converts the month, day and year values into integers.'''
        df['month'] = pd.to_numeric(df['month'],errors='coerce')
        df['year'] = pd.to_numeric(df['year'],errors='coerce')
        df['day'] = pd.to_numeric(df['day'],errors='coerce')
        df_no_null = df.dropna(axis=0,how='any')
        return df_no_null


