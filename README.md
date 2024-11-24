# Multinational Retail Database Project
---
## Project Description
This is a project which retrieves and analyses data on the sales and of a multinational retail company. The project contains code which retrieves the data from multiple sources, cleans it and uploads it to a postgresql database. The files in sql_database_alterations then use sql to prepare the star database schema. Finally, the queries.sql file performs queries to extract information from the database.

## Usage
To use the code and create a postgresql database with it, we must first create a yaml file named db_creds.yaml which contains the credentials of the RDS postgresql database from which to import the user data. Then, after setting up our own postgresql database to receieve the data, we may run the retrieve_clean_upload.py file.
Upon the creation of the database the user must then run each of the scripts in the sql_table_alterations folder while connected to the new database. These may be run in any order, provided that the script primary_foreign_key.sql is run last.
Finally, run the queries.sql file on the same connection to obtain the required insights.
## Files Structure
The file data_extraction.py contains a class DataExtractor which retrieves the data about the customers, card details, products, orders and stores from various sources, including aws s3, pdf files, urls and using the requests module. The data_cleaning.py file contains a DataCleaning class which contains methods to clean each of the DataFrames obtained by the DataExtractor. Thirdly, the data_base_utils.py contains the class DataBaseConnector, which contains methods to inteact with a postgresql database and upload tables to your postgresql database. The file retrieve_clean_upload.py uses these classes to retrieve, clean and upload the data to your postgresql database.
The folder sql_database_alterations contains 7 sql files. Six of these perform alterations on the six tables in the created database, setting the desired datatypes and, in the case of products_table_prep.sql, creating two new columns. The remaining file, primary_foreign_key.sql sets the primary key in each of the dimension tables and the corresponding foreign keys in the orders_table.
The queries.sql file contains SQL code to query the database.
## Licensing
MIT License

Copyright (c) 2024 Michael Gowie

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."