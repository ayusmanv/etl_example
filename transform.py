# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:42:00 2023

@author: ayusman
"""

from ETL import extract
from ETL import load
import pandas as pd

#extraction
data = extract.extract(bucket ='aws bucket name',filename= 'key or the filename', AWS_KEY_ID = 'user AWS Key',AWS_SECRET = 'user AWS secret', region = 'region')

#example of some transformation

'''
The examples are based on a dataset of stock market aroubd the world

Data columns (total 9 columns):
 #   Column     Non-Null Count   Dtype  
---  ------     --------------   -----  
 0   Index      104224 non-null  object 
 1   Date       104224 non-null  object 
 2   Open       104224 non-null  float64
 3   High       104224 non-null  float64
 4   Low        104224 non-null  float64
 5   Close      104224 non-null  float64
 6   Adj Close  104224 non-null  float64
 7   Volume     104224 non-null  float64
 8   CloseUSD   104224 non-null  float64
 
'''
#Adding more meaning to data    
index_country = {'NYA':'New York Stock Exchange','N225':'Nikkei 225','IXIC':'Nasdaq','GSPTSE':'S&P TSX','HSI':'Heng Seng Index',\
                'GDAXI':'Dax Performance Index','SSMI':'Swiss Market Index','TWII':'Taiwan Weighted Index','000001.SS':'Sanghai Stock Exchange',\
                '399001.SZ':'Shenzen Component','N100':'Euronext 100','NSEI':'Nifty 50','J203.JO':'Johannesburg Index'}
data['Index_Name']=data['Index'].replace(index_country)

#Calculating new data
data['Volatality']=data['High']-data['Low']

# converting Date from object type to Datetime type
data['Date'] = pd.to_datetime(data['Date'], format = '%Y-%m-%d', errors='coerce')

#subsetting the data
data_subset = data[data['Index'].isin(['GSPTSE','IXIC','NYA'])]



#load after transformation

load.load(host = 'host_string', database = 'db_name', user = 'username', password = 'pwd', df = data_subset, table_name = 'table_name')


