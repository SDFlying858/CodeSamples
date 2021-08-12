import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import json
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import time
import datetime


remote = create_engine('')
# url = requests.get('http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1')
url = requests.get('http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1')
# make soup 
soup = BeautifulSoup(url.content, "lxml")
newdict = json.loads(str(soup.text))
v = list(newdict.values())
nodelist = []
nodes = newdict['l'][2]['m']
# nodelist builder
for node in nodes:
    nodelist.append(node)

df = pd.DataFrame(nodelist)
# column rename
df.rename(columns={'a':'region', 'c':'lat/long', 'dc':'day_ahead_energy',
                   'dg':'day_ahead_congestion', 'dl':'day_ahead_loss',
                   'dp':'day_ahead_price', 'fc':'real_time_energy',
                   'fg':'real_time_congestion', 'fp':'real_time_price',
                   'fl':'real_time_loss','n':'node_name', 'p':'node_type',
                   'qc':'fifteen_min_energy', 'qg':'fifteen_min_congestion',
                   'ql':'fifteen_min_loss','qp':'fifteen_min_price'},
                   inplace=True)
# column drop
df.drop(columns=['region', 'lat/long','dk', 'fo', 'qk', 'qo', 'do', 't','node_type'],inplace=True)
# test array
array = ['Q510_7_N005', 'C493GEN_7_N003', 'AVSOLAR_7_N008', 'TOT427A_7_N001',
         'CAVLSRGN_7_B1', 'TOPAZC1_7_N021', 'CARRIZO_1_N001', 'T0239_7_N002',
         'TOT148A_7_N001','TOT108_2_N006','ALTAD2_7_N006', 'ALTA6E2_7_N002',
         'TOT162W4_7_N001', 'GLDTGEN_7_N002']

df = df.loc[df['node_name'].isin(array)]
# id matcher from 'nodes_caiso table'
df_nodes = pd.read_sql_table('nodes_caiso',con=remote, index_col='id')
id_nodename = dict(zip(df_nodes.index, df_nodes.node_name))
df['node_id'] = id_nodename.keys()
df['timestamp'] = datetime.datetime.now()
df.drop(columns='node_name',inplace=True)
now = datetime.datetime.now()
df['date'] = int(now.strftime('%Y%m%d'))
df['hour'] = int(now.strftime('%H'))
df['minute'] = int(now.strftime('%M'))
df.to_sql('nodes_pricing',con=remote, if_exists='append',index=False)
