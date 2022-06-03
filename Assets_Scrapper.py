#!/usr/bin/env python
# coding: utf-8

# In[1]: IMPORT LIB AND DEFINE FUNCTIONS
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import sleep

# Import libraries for environment variables
import os
from dotenv import load_dotenv
load_dotenv()

#Wait for HTTP response
def r_wait(response, timeout, timewait):
    timer = 0
    if response.status_code <= 400:
        return response.status_code
    while response.status_code == 204:
        sleep(timewait)
        timer += timewait
        if timer > timeout:
            return 408
            break
        if response.status_code == 200:
            return response.status_code
            break

# Create index request to ingest bulk data into Elasticsearch
def es_request(es_index, tmp_bulk):
    #Create index and ingest data via _bulk
    endpoint=ES_ENDPOINT
    user=ES_USER
    user_pass=ES_PASSWORD
    r_header = {'Content-Type':'application/json'}
    r_body_index = open(ES_INDEX_JSON, "r").read()
    r_body_bulk = tmp_bulk
    #print(r_body_bulk)
    s = requests.Session()
    r_index = s.put(endpoint+es_index.lower(), headers=r_header, auth=(user, user_pass), data=r_body_index)
    r_bulk= s.post(endpoint+'_bulk', headers=r_header, auth=(user, user_pass), data=r_body_bulk)
    #print(r_bulk.text)
    return r_bulk


# Create bulk data request to ingest bulk data by batches into Elasticsearch
def es_bulk(es_ticker, es_df_table, es_index_name):
    #Create JSON "data" list with stock records
    string_raw = '{"data":'+ es_df_table.to_json(orient="records") + '}'
    json_parsed = json.loads(string_raw)

    #Create _bulk body data for ES
    es_index = es_index_name
    tmp_bulk = ''
    i = 0
    
    #Bulk in packages
    for tmp_record in json_parsed['data']:
        i = i + 1
        doc_id = es_ticker.lower() + '_' + tmp_record['Date']
        tmp_index = '{ "index" : { "_index" : ' + '"'+es_index +'"'+ ', "_id" : ' +'"'+ doc_id +'"' + '}}\n'
        #tmp_record.update(json_ticker)
        tmp_record = ''+ json.dumps(tmp_record) + '\n'
        tmp_bulk = tmp_bulk + tmp_index + tmp_record
        if i == 1000:
            es_response = es_request(es_index, tmp_bulk)
            r_response = r_wait(es_response, 10, 0.0010)
            print(r_response)
            tmp_bulk = ''
            i = 0
    if i > 0:
        es_request(es_index, tmp_bulk)
            
        
    #print(tmp_bulk)



# In[2]: Set variables and get list of Assets

# Import environment variables
ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USER = os.environ["ES_USER"]
ES_PASSWORD = os.environ["ES_PASSWORD"]
ES_INDEX_JSON= os.environ["ES_INDEX_JSON"]
ES_INDEX_NAME= os.environ["ES_INDEX_NAME"]

URL_DOWNLOAD = os.environ["URL_DOWNLOAD"]
URL_PERIOD1 = os.environ["URL_PERIOD1"]
URL_PERIOD2 = os.environ["URL_PERIOD2"]
URL_OTHER = os.environ["URL_OTHER"]

#Get current date and timeframe
current_date=datetime.now()
time_range=timedelta(days=14)
start_date=current_date - time_range
current_date_s = int(current_date.timestamp())
start_date_s = int(start_date.timestamp())
#print('current seconds:', current_date_s, 'start seconds:', start_date_s)
#print("start:", start_date, "current:", current_date, "range:", time_range)

#Get and convert Ticker list to Dataframe
dir_path = os.path.dirname(os.path.realpath(__file__))
csv_path = dir_path + '/Assets.csv'
df_assets = pd.read_csv(csv_path, sep=',')



# In[3]: GET, PROCESS AND INGEST DATA TO ELASTICSEARCH based on list of Assets

# Calculate and insert returns
tickers = df_assets.Ticker.tolist()

for ticker in tickers:
    sleep(2)
    print(ticker)
    url_ticker=ticker
    url=URL_DOWNLOAD+url_ticker+URL_PERIOD1+str(start_date_s)+URL_PERIOD2+str(current_date_s)+URL_OTHER
    try:
         df_table=pd.read_csv(url)
    

         #Add return to every column except 'Date' a RETURN column
         table_columns = df_table.columns.values.tolist()
         table_columns.remove('Date')
         for table_column in table_columns:
             df_table['Return ' + table_column] = df_table[table_column] / df_table[table_column].shift(1) - 1
        
         #Add columns for Year, Month and Day    
         df_table['Year'] = pd.DatetimeIndex(df_table['Date']).year
         df_table['Month'] = pd.DatetimeIndex(df_table['Date']).month
         df_table['Day'] = pd.DatetimeIndex(df_table['Date']).day
    
         #Add other columns
         df_table['Return Open[0]/Close[-1]'] = df_table['Open'] / df_table['Close'].shift(1) - 1
         df_table['Return Close[0]/Open[0]'] = df_table['Close'] / df_table['Open'] - 1
         df_table['Dollar Volume'] = ((df_table['Open'] + df_table['Close']) * df_table['Volume']) / 2
         df_table['Ticker'] = ticker
         df_table['Range'] = (df_table['High'] - df_table['Low']) / df_table['Open']
         df_table['High Swing'] = (df_table['High'] - df_table['Open']) / df_table['Open']
         df_table['Low Swing'] = (df_table['Low'] - df_table['Open']) / df_table['Open']
    
         #Merge with Input file
         df_table = pd.merge(df_table, df_assets, left_on='Ticker', right_on='Ticker', how='left')
    
         df_table = df_table.fillna("")
         es_bulk(ticker, df_table, ES_INDEX_NAME)
    
    except Exception as e:
         print("error")
         print('Failed: '+ str(e))





