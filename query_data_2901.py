#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import psycopg2


'''
connect to the dATABASE ON DOCKER and load data in a dataframe
'''
def main(params):
  user = params.user
  password = params.password
  host = params.host
  port = params.port
  db = params.db
  table_name = params.table_name
  #url = params.url

  conn = psycopg2.connect(database=db,
                          host=host,
                          user=user,
                          password=password,
                          port=port)
  print(conn)
  cursor = conn.cursor()
  '''
  0, 1, datetime.datetime(2021, 1, 1, 0, 30, 10), datetime.datetime(2021, 1, 1, 0, 36, 12), 1, 2.1, 1, 'N', 142, 43, 2, 8.0, 3.0, 0.5, 0.0, 0.0, 0.3, 11.8, 2.5)

  '''
  #thedate = '2019-09-18'''





  # We just need to turn it into a pandas dataframe

  cursor.execute("SELECT * FROM yellow_taxi_trips")
  tupples = cursor.fetchall()
  #print(tupples)
  cursor.execute("select column_name from information_schema.columns where table_name = 'yellow_taxi_trips'")
  column_name = cursor.fetchall()
  print(column_name)
  column_name=  ['index', 'VendorID' , 'lpep_pickup_datetime','lpep_dropoff_datetime' ,
   'store_and_fwd_flag',
   'RatecodeID',
   'PULocationID',
   'DOLocationID',
   'passenger_count',
   'trip_distance',
   'fare_amount',
   'extra', 'mta_tax',
   'tip_amount',
   'tolls_amount', 'ehail_fee', 'improvement_surcharge',
    'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'
  ]
  print(len(column_name))
  j=0
  for i in column_name:
    print(i + ' : ' + str(j))
    j=j+1

  #df = pd.DataFrame(tupples, columns=column_name)

  cursor.close()
  column_names=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
  df = pd.DataFrame(tupples, columns=column_name)
  print(df.head())
  print(df.shape)

  ss = df['lpep_pickup_datetime'][0] #first row

  pickup_day_list = df['lpep_pickup_datetime']
  pickup_day_list = [str(x)[:10] for x in pickup_day_list]
  dropoff_day_list = df['lpep_dropoff_datetime']
  dropoff_day_list = [str(x)[:10] for x in dropoff_day_list]

  df['pickup_day']=pickup_day_list
  df['dropoff_day'] = dropoff_day_list
  print(df.head())

  #filterinfDataframe = df[(df['pickup_day'] == '2019-09-18') & (df['dropoff_day'] =='2019-09-18')]
  filterinfDataframe = df[(df['pickup_day'] == '2019-09-18') & (df['dropoff_day'] =='2019-09-18')]

  print(filterinfDataframe.shape)
  print(dropoff_day_list[0], ' ', len(dropoff_day_list[0]))

  '''
  Question 5. Three biggest pick up Boroughs
  '''
  fn = 'taxi+_zone_lookup.csv'
  borrows = readCSV(fn)
  #longerTripDay(df)

  filterinfDataframe = df[(df['pickup_day'] == '2019-09-18') ]
  #biggestPickUpBoroughs(filterinfDataframe, borrows)
  merged_df = mergeDFs(borrows, filterinfDataframe)
  print(merged_df.shape)
  print(merged_df.head())
  biggestPickUpBoroughs(merged_df)

  '''
  Question 6. Largest tip
  '''
  print('======= Question 6. Largest tip ===============')

  month_list = [str(x)[:7] for x in pickup_day_list]
  df['year_month'] =month_list

  filterinfDataframe = df[(df['year_month'] == '2019-09') ]
  print(filterinfDataframe.head())
  print(filterinfDataframe.shape)

  returned_DF = mergeDFs_all_cols(borrows, filterinfDataframe)
  print(returned_DF.shape)
  print(returned_DF.head())
  '''  in the zone name Astoria  '''
  filterinfDataframe = returned_DF[(returned_DF['Zone'] == 'Astoria')]
  print(filterinfDataframe.head())
  print(filterinfDataframe.shape)
  largestTip(filterinfDataframe, borrows)



def largestTip(df,borrows ):
  print(df.head())
  df.sort_values(by='tip_amount', ascending=False, inplace=True)
  print(df['tip_amount'].head())
  print(df.iloc[0])
  locationID= int(df.DOLocationID.iloc[0])
  print(locationID)
  print(borrows.head())
  print(borrows.query('LocationID == '+ str(locationID)))



def longerTripDay(df):
  #Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
  df['trip_duration'] = df[3]-df[2]
  print(df.head())
  df.sort_values(by='trip_duration', ascending=False, inplace=True)
  print(df.head())

def biggestPickUpBoroughs(df):
  ''' Considermlpep_pickup_datetime in '2019-09-18' and ignoring Borough
  has Unknown
  '''
  print(df['Borough'].unique())
  print(df.shape)

  print(df.groupby('Borough').agg({'total_amount': 'sum'}))
  df2 = df.groupby('Borough').agg({'total_amount': 'sum'})
  print(df2.head())
  df2.sort_values(by='total_amount', ascending=False, inplace=True)
  print(df2.head())
  print(df2.index)
  print(type(df2))
  df_index=df2.index
  print(type(df_index))
  print(df_index[0:3])
  index_list= df_index[0:3].to_list()
  print(index_list)
  # borrows_ids =df2.to_frame()
  # print(type(borrows_ids))
  #



def readCSV(fn):
  borrows_df = pd.read_csv(fn)
  print(borrows_df.shape)
  print(borrows_df.head())
  bdf = borrows_df.groupby(['Borough'])
  print(type(bdf))
  print(bdf.groups.keys())
  for key, item in bdf:
    print(key)

    print(bdf.get_group(key), "\n\n")
    key_group = bdf.get_group(key)
    print(type(key_group))
    print(key_group['LocationID'])



  return borrows_df

def mergeDFs(df1,df2):

  # Define two dataframes
  #df1 = pd.DataFrame({"key": ["A", "B", "C", "D"], "value1": [1, 2, 3, 4]})
  print(df1.columns)
  df1['PULocationID'] = df1['LocationID']

  print(df1.columns)
  print(df1.shape)

  #df2 = pd.DataFrame({"key": ["B", "D", "E", "F"], "value2": [5, 6, 7, 8]})
  '''
  Index(['LocationID', 'Borough', 'Zone', 'service_zone', 'PULocationID'], dtype='object')
Index(['index', 'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
       'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
       'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
       'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
       'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge',
       'pickup_day', 'dropoff_day'],
      dtype='object')

  '''
  print(df2.columns)
  print(df2.shape)

  # Perform the merge
  merged_df = pd.merge(df1, df2, on="PULocationID", how="outer")
  print(merged_df.shape)
  print(merged_df.columns)

  return merged_df[['Borough','total_amount']]

  # Show the resulting
  #print(merged_df)

def mergeDFs_all_cols(df1,df2):

  # Define two dataframes
  #df1 = pd.DataFrame({"key": ["A", "B", "C", "D"], "value1": [1, 2, 3, 4]})
  print(df1.columns)
  df1['PULocationID'] = df1['LocationID']
  print(df1.columns)
  print(df1.shape)
  print(df2.columns)
  print(df2.shape)

  # Perform the merge
  merged_df = pd.merge(df1, df2, on="PULocationID", how="outer")
  print(merged_df.shape)
  print(merged_df.columns)

  return merged_df


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

  parser.add_argument('--user', required=True, help='user name for postgres')
  parser.add_argument('--password', required=True, help='password for postgres')
  parser.add_argument('--host', required=True, help='host for postgres')
  parser.add_argument('--port', required=True, help='port for postgres')
  parser.add_argument('--db', required=True, help='database name for postgres')
  parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
  parser.add_argument('--url', required=True, help='url of the csv file')

  args = parser.parse_args()

  main(args)