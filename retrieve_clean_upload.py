import database_utils as util
import data_cleaning as clean
import data_extraction as extract

connector = util.DataBaseConnector()
cleaner = clean.DataCleaning()
extractor = extract.DataExtractor()


pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
products_s3_address = 's3://data-handling-public/products.csv'
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
individual_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

def upload_user():
    user_df = extractor.read_rds_table(connector, 'legacy_users')
    clean_user_df = cleaner.clean_user_data(user_df)
    connector.upload_to_db(clean_user_df, 'dim_users')

def upload_card_details():
    card_df = extractor.retrieve_pdf_data(pdf_url)
    clean_card_df = cleaner.clean_card_data(card_df)
    connector.upload_to_db(clean_card_df, 'dim_card_details')


def upload_stores():
    stores_df = extractor.retrieve_stores_data(individual_store_endpoint, header_dict)
    clean_stores_df = cleaner.clean_store_data(stores_df)
    connector.upload_to_db(clean_stores_df, 'dim_store_details')

def upload_products():
    products_df = extractor.extract_from_s3(products_s3_address)
    clean_products_df = cleaner.clean_product_data(products_df)
    connector.upload_to_db(clean_products_df, 'dim_products')
    
def upload_orders():
    orders_df = extractor.read_rds_table(connector, 'orders_table')
    clean_orders_df = cleaner.clean_orders_data(orders_df)
    connector.upload_to_db(clean_orders_df,'orders_table')

def upload_dates():
    date_df = extractor.extract_dates_from_s3()
    clean_date_df = cleaner.clean_dates_data(date_df)
    connector.upload_to_db(clean_date_df,'dim_date_time')



def upload_data_base():
    upload_card_details()
    upload_dates()
    upload_orders()
    upload_products()
    upload_stores()
    upload_user()


upload_data_base()


