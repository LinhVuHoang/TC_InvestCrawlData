import pandas as pd
import requests
import csv
from tqdm import tqdm
import asyncio
import aiohttp
import time
from datetime import datetime,timedelta
import schedule
import os
import logging
import sys
import math
import psutil
import pyodbc
import sqlalchemy
from airflow import DAG
import json
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum 
default_args = {
    'start_date': pendulum.datetime(2023,9,27,tz="Asia/Ho_Chi_Minh"),
    'owner':'linhhv',
    'retries':1,
    'retry_delay': timedelta(minutes=10)
}


def db_connection(folder_name):
    conn = None
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal;DATABASE=master;UID=sa;PWD=Docker@123;TrustServerCertificate=yes;',autocommit=True)
    except Exception as e:
        print("Lỗi to đùng đùng")
        print(e)
    return conn
list_empty=[]
Code_stock=[]
count=0
def get_code_invest(ti):
    with open('data/list_code_invest.txt', 'r') as file:
        code_list = [line.strip() for line in file.readlines()]
    print(code_list)
    ti.xcom_push(key='code_list',value=code_list)
async def request_link(code,semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            list_link=[]
            try:
                while True:
                    async with session.get(f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{code}/his/paging?page=0&size=100&headIndex=-1') as response:
                        if response.status ==200:
                            js_data = await response.json()
                            total_index = js_data['total']
                            if total_index !=0:
                                pages = (total_index //100) +1
                                for i in range(pages):
                                    try:
                                        link = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{code}/his/paging?page={i}&size=100&headIndex=-1'
                                        list_link.append(link)
                                    except:
                                        pass
                            return list_link
                        else:
                            break
            except Exception as e:
                pass
            return list_link

async def crawl_link(item,semaphore):
    global count
    async with semaphore:
        async with aiohttp.ClientSession() as session: 
            df = pd.DataFrame() 
            for i in item:
                while True:  
                    try:
                        async with session.get(i) as response: 
                            if response.status == 200:
                                js_data = await response.json()
                                df1 = pd.json_normalize(js_data['data'])
                                df = pd.concat([df, df1], ignore_index=True)
                                break  
                    except Exception as e:
                        crawl_job_error_nums.inc()
                        print(e)
                        pass
            return df           
count=0
async def crawl(Code_stock):
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    folder_name = 'etl_ingest_time_'+timestamp
    print(folder_name)
    query_db = f"CREATE DATABASE [{folder_name}]"
    url = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal;DATABASE='+folder_name+';UID=sa;PWD=Docker@123;TrustServerCertificate=yes;'
    conn = db_connection(folder_name)
    cursor = conn.cursor()
    cursor.execute(query_db)
    cursor.execute(f"USE [{folder_name}]")
    global list_empty
    global count
    semaphore = asyncio.Semaphore(48)
    time1 = time.time()
    batch_size = 50
    print(len(Code_stock))
    num_batches = math.ceil(len(Code_stock) / batch_size)
    print(num_batches)
    os.makedirs(f'/opt/airflow/data/{folder_name}',exist_ok=True)
    list_df =[]
    try:
        for i in tqdm(range(num_batches)):
            time3 = time.time()
            Code_new_stock = Code_stock[i * batch_size:(i + 1) * batch_size]
            print(Code_new_stock)
            list_df = await asyncio.gather(*[crawl_link(items,semaphore) for items in await asyncio.gather(*[request_link(code,semaphore) for code in Code_new_stock])])
            print(list_df)
            print("hello")
            print(len(list_df))
            for j in tqdm(range(len(list_df))):
                if (len(list_df[j])) ==0:
                    print(f'{Code_stock[j]} is empty') 
                else:
                    print(list_df[j])
                    sql_query = f"""CREATE TABLE [data_{Code_new_stock[j]}](
                    p decimal(10,2),
                    v int,
                    cp decimal(10,2),
                    rcp decimal(10,2),
                    a text,
                    ba decimal(10,2),
                    sa decimal(10,2),
                    hl bit,
                    pcp decimal(10,2),
                    t text
                    ) """
                    cursor.execute(sql_query)
                    print(url)
                    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(url))
                    list_df[j].to_sql(f'data_{Code_new_stock[j]}',engine, if_exists='replace', index=False)
                    list_df[j].to_csv(f'/opt/airflow/data/{folder_name}/data_{Code_new_stock[j]}.csv',index=False)
                    s3_hook = S3Hook(aws_conn_id="minio_s3_conn")
                    s3_hook.load_file(
                        filename = f'/opt/airflow/data/{folder_name}/data_{Code_new_stock[j]}.csv',
                        key=f"{folder_name}/data_{Code_new_stock[j]}.csv",
                        bucket_name ="airflow",
                        replace=True
                    )
            
            count=count+1
            print(f"chay lan {count}")
            time4 = time.time()
            print(f'Took {time4-time3:.2f} s')
        time2= time.time()

        print(f'DONE CRAWL IN {time2-time1:.2f} s')
    except Exception as ex:
        print(ex)                              
def run_my_async_task(ti):
    Code_stock = ti.xcom_pull(task_ids='get_code_invest',key='code_list')
    Code_stock=Code_stock[0:10]
    print(Code_stock)
    asyncio.run(crawl(Code_stock))
with DAG(
    default_args=default_args,
    dag_id='crawl_dag_v01',
    description='Crawl invest data',
    # start_date=datetime(2023, 9, 28),
    schedule_interval='12 15 * * *'
) as dag:
    task1 = PythonOperator(
        task_id ='get_code_invest',
        python_callable=get_code_invest
    )
    task2 = PythonOperator(
        task_id ='run_my_async_task',
        python_callable=run_my_async_task
    )
    task1 >> task2