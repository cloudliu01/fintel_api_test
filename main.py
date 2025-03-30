import os
import json
import requests
import pytz
import logging
import pandas as pd
from datetime import datetime
from warnings import warn
from termcolor import colored

from common import  OutputPathSingleton, convert_timestamp_columns, top_shareholders_by_symbol, parse_formatted_file_date
from common import RETRIEVED_JSON_PATH, DEBUG

from stock_analyzer import StockOwnershipAnalyzer
from stock_ownership import StockOwnershipAPI
from stock_price import StockDataManager
from db_data_handler import DBDataHandler


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if DEBUG:
    # warning with colored output
    logging.warning(colored("Running in DEBUG mode!!!", "yellow"))

# Constants
SISTER_7_SLUGS = ['googl', 'amzn', 'meta', 'nvda', 'aapl', 'msft', 'tsla', 'intc', 'qcom', 'mu']


#if DEBUG:
#    SISTER_7_SLUGS = ['intc']

#SISTER_7_SLUGS = ['tsla']
X_API_KEY = os.environ.get("X-API-KEY")
FINTEL_DB_NAME = os.environ.get("FINTEL_DB_NAME")
FINTEL_DB_USER = os.environ.get("FINTEL_DB_USER")
FINTEL_DB_PASS = os.environ.get("FINTEL_DB_PASS")
FINTEL_DB_HOST = os.environ.get("FINTEL_DB_HOST")
FINTEL_DB_PORT = os.environ.get("FINTEL_DB_PORT")



if __name__ == "__main__":
    results_dir = OutputPathSingleton.get_path()

    db_file = os.path.abspath(os.path.join(results_dir, '../stocks_price.db'))
    manager = StockDataManager(db_url=f'sqlite:///{db_file}', symbols=SISTER_7_SLUGS, default_start_date="2024-01-01")
    manager.update_data()
    df_stock_price = manager.get_data()

    db_handler_stock_price = DBDataHandler(FINTEL_DB_NAME, FINTEL_DB_USER, FINTEL_DB_PASS, FINTEL_DB_HOST, int(FINTEL_DB_PORT),
                         ['symbol', 'date'])
    db_handler_stock_price.initialize_table(df_stock_price, 'stock_price')
    db_handler_stock_price.upload_dataframe(df_stock_price, 'stock_price' )


    api_client = StockOwnershipAPI(X_API_KEY, base_url='https://api.fintel.io/data/v/0.0/so/us/')
    api_client.retrieve_sisters_data(slug_list=SISTER_7_SLUGS)
    #api_client = StockOwnershipAPI(X_API_KEY, base_url='https://api.fintel.io/data/v/0.0/i/')
    #api_client.retrieve_sisters_data(slug_list=['vanguard-group'])
    sa = StockOwnershipAnalyzer(results_dir, slug_list=SISTER_7_SLUGS)
    sa.load_data()
    df = sa.df
    df.columns = df.columns.str.lower() # changed column names to lowercase to fit the DB schema
    df = convert_timestamp_columns(df, ['filedate', 'effectivedate', 'formattedfiledate'])



    result = top_shareholders_by_symbol(df, top_n=50)
    api_client_fund = StockOwnershipAPI(X_API_KEY, base_url=f'https://api.fintel.io/data/v/0.0/i/')
    slug_list_fund = []
    for stock_symbol, df_top_fund in result.items():
        for index, row in df_top_fund.iterrows():
            slug_list_fund.append(row['slug'])

    slug_list_fund = list(set(slug_list_fund))
    print(f'Total unique slugs : {len(slug_list_fund)}')
    api_client_fund.retrieve_sisters_data(slug_list=slug_list_fund)

    sa_s = StockOwnershipAnalyzer(results_dir, slug_list=slug_list_fund, mode='fund')
    sa_s.load_data()
    df_s = sa_s.df
    df_s.columns = df_s.columns.str.lower() # changed column names to lowercase to fit the DB schema
    df_s['filedate'] = pd.to_datetime( df_s['filedate'].apply(lambda x: f"{x[0]}-{x[1]:02d}-{x[2]:02d}")).replace({pd.NaT: None})
    df_s['effectivedate'] = pd.to_datetime( df_s['effectivedate'].apply(lambda x: f"{x[0]}-{x[1]:02d}-{x[2]:02d}"))
    df_s['formattedfiledate'] = df_s['formattedfiledate'].apply(parse_formatted_file_date)


    # initialize the DB handler
    db_handler = DBDataHandler(FINTEL_DB_NAME, FINTEL_DB_USER, FINTEL_DB_PASS, FINTEL_DB_HOST, int(FINTEL_DB_PORT))
    db_handler.initialize_table(df, 'stock')
    db_handler.upload_dataframe(df, 'stock' )

    db_handler_fund = DBDataHandler(FINTEL_DB_NAME, FINTEL_DB_USER, FINTEL_DB_PASS, FINTEL_DB_HOST, int(FINTEL_DB_PORT),
                         ['exchangesymbol', 'slug', 'formtype', 'filedate', 'effectivedate', 'shares', 'shareschange', 'value'])
    db_handler_fund.initialize_table(df_s, 'fund')
    db_handler_fund.upload_dataframe(df_s, 'fund' )



    print('Done')
