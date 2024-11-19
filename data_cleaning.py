import pandas as pd
import numpy as np
import re
from dateutil.parser import parse

class DataCleaning:
    def clean_user_data(self,df):
        df_rep = df
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan,'GGB':'GB'})
        country_code_mask = df_rep.country_code.isin(['GB','DE','US'])
        df_rep = df_rep[country_code_mask]
        df_rep = df_rep.dropna(axis=0,how='any')
        df_rep.join_date = df_rep.join_date.apply(parse)
        df_rep.join_date = pd.to_datetime(df_rep.join_date, errors='coerce')
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_card_data(self,df):
        df_rep = pd.DataFrame()
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan, })
        df_rep = df_rep.dropna(axis=0,how='any')
        card_num_mask = ~df_rep['card_number'].astype(str).str.isupper()
        #print(card_num_mask.head())
        df_rep = df_rep[card_num_mask]
        #df_rep.date_payment_confirmed = df_rep.date_payment_confirmed.apply(parse)
        #df_rep.date_payment_confirmed = pd.to_datetime(df_rep.date_payment_confirmed, errors='coerce')
        #df_rep.drop_duplicates(['card_number'], inplace=True)
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_store_data(self, df):
        #df.drop(axis=1, columns=['lat'], inplace=True)
        country_code_mask = df['country_code'].isin(['DE','GB','US'])
        df = df[country_code_mask]
        df.staff_numbers = df.staff_numbers.str.strip()
        df.staff_numbers = df.staff_numbers.replace({r'[^0-9]+':''},regex=True)
        return df
    def convert_product_weights(self,df):
        def convert_weight(weight_str):
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
        df_rep = pd.DataFrame()
        for column in list(df):
            df_rep[column] = df[column].replace({'NULL':np.nan})
        for column in list(df):
            mask = df_rep[column].isna()
            #print(df_rep[mask])
        
        df_rep._set_value(1779,'weight',0.077)
        df_rep._set_value(1841,'weight',0.454)
        df_no_null = df_rep.dropna(axis=0,how='any')
        return df_no_null
    def clean_orders_data(self,df):
        df.drop(['1','first_name','last_name','level_0'],axis=1, inplace=True)
        return df
    def clean_dates_data(self,df):
        df['month'] = pd.to_numeric(df['month'],errors='coerce')
        df['year'] = pd.to_numeric(df['year'],errors='coerce')
        df['day'] = pd.to_numeric(df['day'],errors='coerce')
        df_no_null = df.dropna(axis=0,how='any')
        return df_no_null


