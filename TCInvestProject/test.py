import pandas as pd
import requests
import csv
from tqdm import tqdm
import asyncio
import aiohttp
import time
import datetime as dt
import schedule
import os
import logging
import sys
import math
import psutil
import pyodbc
import sqlalchemy
from prometheus_client import start_http_server,Gauge,Counter,Summary,Histogram,Enum
# #thiết lập cấu hình logging
# Imports

# Create SparkSession
logging.basicConfig(filename='/Users/hoangvulinh/Documents/TCInvestProject/log/CrawlInvest.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
e = Enum('my_state_crawl_job','Description of enum',states=['Starting','Running','Stopped']) # state of crawl job task
crawl_job_duration = Gauge('crawl_job_duration_seconds','Duration of crawl job in seconds') # time for crawl task done
crawl_job_error_link = Counter('Crawl_job_error_links','Number of errors in crawl link job') # number of errors for crawl link task
crawl_job_error_nums = Counter('Crawl_job_error_numbers','Number of errors in crawl data job') #number of errors for crawl data task
cpu_usage = Gauge('cpu_usage_percentage', 'CPU Usage Percentage') # cpu usage
ram_usage = Gauge('ram_usage_bytes', 'RAM Usage in Bytes') #ram usage

# crawl_job_size = Histogram('Crawl_job_size','Size of crawl job') # size of crawl job task
crawl_code_done = Gauge('Crawl_code_done','Number of code crawled')
crawl_job_duration_all = Gauge('Crawl_job_all','Duration of all batch crawl')
crawl_all_time = Gauge('Crawl_all_time','Time for crawl done')
day_crawl = Counter('Day_crawl','Number of crawl days')
url_code = 'https://apiextaws.tcbs.com.vn/poseidon/v1/categories?types=DWL'
token='eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW5fc2VydmljZSIsImV4cCI6MTY5NDgwMzYxMCwianRpIjoiIiwiaWF0IjoxNjk0NzYwNDEwLCJzdWIiOiIxMDAwMDU5NzA2NiIsImN1c3RvZHlJRCI6IjEwNUM3MDM4MDAiLCJlbWFpbCI6InJlZHRvbmxpbmhAZ21haWwuY29tIiwicm9sZXMiOlsiY3VzdG9tZXIiXSwic2NvcGVzIjpbImFsbDphbGwiXSwic3RlcHVwX2V4cCI6MCwic290cF9zaWduIjoiIiwiY2xpZW50X2tleSI6IjEwMDAwNTk3MDY2LlhzYkFpaHJTQXI1dGsyUU14RjU0Iiwic2Vzc2lvbklEIjoiMjczMmI3YzItZDRjZS00YjA4LWJkOTctYzU0ZjRjYjdjZWQwIiwiYWNjb3VudF9zdGF0dXMiOiIxIiwib3RwIjoiIiwib3RwVHlwZSI6IiIsIm90cFNvdXJjZSI6IlRDSU5WRVNUIiwib3RwU2Vzc2lvbklkIjoiIn0.lZDlcdrK-9B8PwHtHYyL4t7m8WDlESlhMeJ5HhprWaG0gLavc5M0QuPSoj1_mLh5vXwjjQX4ksC3C0MNPe0g3VQVD2fNR31NcYRAzUIGUdrLp_A2jukXI0FL84gzJ0bdLey5ZJBfJYwGadVdMpwiZ5SWKq92ScSQHkRnmzQF6e4sORF_0SIczN6DlbH6d136bSIMnNZbwTEocs8NuZItpka6Bku6IHHx5GFF5dSxlhpkL3VAMpDsKv1gOHNyp_lIfQigk4l23-vPOeeWFWJ7BEfqT2d1oTlnD4pOS1O-gCx6dZyoqhMAqhv-Fs_-Z3UXJYB1XOvm71O1L2qHqHtY-Q'
headers={
    'Authorization': f'Bearer {token}'
}
list_empty=[]
def update_metrics():
    # Lấy thông tin tài nguyên CPU và RAM
    cpu_percent = psutil.cpu_percent(interval=1)  # Tỷ lệ sử dụng CPU trong 1 giây
    ram_bytes = psutil.virtual_memory().used  # Số byte RAM đã sử dụng

    # Cập nhật metric Prometheus
    cpu_usage.set(cpu_percent)
    ram_usage.set(ram_bytes)

def db_connection(folder_name):
    conn = None
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;UID=sa;PWD=Docker@123;',autocommit=True)
        logging.info("Connect database success!")
    except Exception as e:
        print(e)
        logging.error("Connect database ")
    return conn
Code_stock=[]
count=0
# Giới hạn tốc độ gửi yêu cầu (ví dụ: 3 yêu cầu mỗi giây)
async def request_link(code,semaphore):
    # global count
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            list_link = []
            try:
                while True:
                    async with session.get(f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{code}/his/paging?page=0&size=100&headIndex=-1') as response: # đảm bảo rằng phiên sẽ được đóng đúng cách sau khi khối mã của nó được thực thi
                        if response.status ==200:
                            js_data = await response.json() # chờ response trả về
                            number_items = js_data['total']
                            if number_items !=0:
                                total_index = js_data['total']
                                pages = (total_index // 100) + 1
                                for i in range(pages):
                                        try:
                                            link = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{code}/his/paging?page={i}&size=100&headIndex=-1'
                                            list_link.append(link)
                                        except:
                                            pass
                                logging.info('request link from code success!')
                                return list_link
                            else:
                                logging.warning('Code is unavailable or link is unavailable')
                                break  # Kết thúc nếu không có dữ liệu             
            except Exception as e:
                crawl_job_error_link.inc()
                print(e)
                logging.error('Error when request link')
                pass
            return list_link  # Trả về danh sách liên kết rỗng nếu không thành công sau số lần thử lại
        
# await request_link('ACV')

async def crawl_link(item,semaphore): #hàm async crawl_link
    global count
    async with semaphore:
        async with aiohttp.ClientSession() as session: #giao dịch http bất đồng bộ như trên
            df = pd.DataFrame() #tạo dataframe rỗng
            for i in item:
                while True:  
                    try:
                        async with session.get(i) as response: 
                            if response.status == 200:
                                js_data = await response.json()
                                df1 = pd.json_normalize(js_data['data'])
                                df = pd.concat([df, df1], ignore_index=True)
                                logging.info('Crawl data from list link follow code success!')
                                break  # Thoát khỏi vòng lặp nếu yêu cầu thành công
                    except Exception as e:
                        crawl_job_error_nums.inc()
                        print(e)
                        logging.warning('Can not get data or link is unavailable')
                        pass
            return df
count=0
async def crawl():
    day_crawl.inc()
    timestamp = dt.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    folder_name = 'etl_ingest_time_'+timestamp
    print(folder_name)
    query_db = f"CREATE DATABASE [{folder_name}]"
    url = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE='+folder_name+';UID=sa;PWD=Docker@123;'
    # os.makedirs(f'/code/CrawlDataStreamingInvest/{folder_name}',exist_ok=True)
    conn = db_connection(folder_name)
    cursor = conn.cursor()
    cursor.execute(query_db)
    cursor.execute(f"USE [{folder_name}]")
    global list_empty
    global count
    semaphore = asyncio.Semaphore(48)
    time1 = time.time()
    batch_size = 50
    num_batches = math.ceil(len(Code_stock) / batch_size)
    print(num_batches)
    update_metrics()
    e.state('Running')
    crawl_code_done.set(0)
    try:
        for i in tqdm(range(num_batches)):
            time3 = time.time()
            Code_new_stock = Code_stock[i * batch_size:(i + 1) * batch_size]
            list_df =[]
            list_df = await asyncio.gather(*[crawl_link(items,semaphore) for items in await asyncio.gather(*[request_link(code,semaphore) for code in Code_new_stock])])
            crawl_code_done.set(0)
            for j in tqdm(range(len(list_df))):
                if (len(list_df[j])) ==0:
                    logging.info(f'{Code_stock[j]} is empty') 
                else:
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
                    # list_df[j].to_sql(f'data_{Code_new_stock[j]}', conn, if_exists='replace', index=False)
                    list_df[j].to_sql(f'[data_{Code_new_stock[j]}]',engine, if_exists='replace', index=False)
                    # conn.commit()
                    # list_df[j].to_csv(f'/code/CrawlDataStreamingInvest/{folder_name}/data_{Code_new_stock[j]}.csv',index=False)
                    logging.info(f'{Code_stock[j]} is crawled')
                    crawl_code_done.inc()
            count=count+1
            print(f"chay lan {count}")
            time4 = time.time()
            crawl_job_duration.set(time4-time3)
            print(f'Took {time4-time3:.2f} s')
        time2= time.time()
        new_time = math.ceil(time2-time1)
        crawl_all_time.set(new_time)
        print(f'DONE CRAWL IN {time2-time1:.2f} s')
        logging.info("Done crawl")
    except Exception as ex:
        print(ex)
        logging.error("Error while crawl") 
   
def crawl_job():  
    asyncio.run(crawl())
    time.sleep(5)
def schedule_jobs(): 
    
    logging.info("Run schedule")
    schedule.every(30).seconds.do(crawl_job)
    # schedule.every().day.at("14:10").do(crawl_job)
    # schedule.every().day.at("16:00").do(crawl_job)
    # schedule.every().day.at("21:00").do(crawl_job)
    # schedule.every().day.at("00:50").do(crawl_job)
    while True:
        schedule.run_pending()
        
if __name__=="__main__":
    try:
        logging.info('Start program')
        e.state('Starting')
        start_http_server(8000)
        time1 = time.time()
        response_code = requests.get(
            url_code,
            headers=headers
        )
        result_code = response_code.json()
        list_code = result_code['category'][0]['ListStockCode']               
        print(len(list_code))
        Code_stock=list_code[0:5]
        crawl_job()
    except Exception as ex:
        print(ex)
        e.state('Stopped')
        logging.warning('Token expired')
        print("Response 405")
# * được sử dụng để unpack ( giải nén ) danh sách kết quả từ các coroutine 

