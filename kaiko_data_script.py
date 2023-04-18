import pandas as pd
import requests 
import datetime
import math
import time
from dotenv import load_dotenv
import os,sys
import json
sys.path.append(os.path.join(os.path.abspath(__file__),"./config.py"))
from config import query
load_dotenv()

api_key = os.getenv('KAIKO_APIKEY')


kaiko_symbol = "eth/usdt"
base_url = "https://us.market-api.kaiko.io/v2/data/trades.v1/"
url_ext = "spot_direct_exchange_rate/" + kaiko_symbol

def stats(start_time,end_time,chunk_duration,query_duration):
    result = {
        "total_duration":end_time.timestamp() - start_time.timestamp(),
        "chunk_iterations":math.ceil((end_time.timestamp() - start_time.timestamp())/chunk_duration),
        "query_iterations":math.ceil(chunk_duration/query_duration),
    }
    result["total_iterations"] = result["chunk_iterations"] * result["query_iterations"]

    return result 

def to_date_string(df:pd.DataFrame,columns):
    if isinstance(columns,str):
        columns = [columns]
    for column in columns:
        df[column] = pd.to_datetime(df[column],dayfirst=True,unit='ms',utc=True).astype(str)
    return df

def get_data_historical(query_start:datetime.datetime,query_end:datetime.datetime):
    header = {
        "X-Api-Key": api_key,
        "timeout": str(10*1000),

    }
    query_params = {
      "sources": query["sources"],
      "start_time": str(query_start.isoformat())+"Z",
      "end_time": str(query_end.isoformat())+"Z",
      "interval": query["interval"],
      "sort": query["sort"],
      "page_size":query["page_size"]
    }
    try:
        return requests.get(url=base_url+url_ext, headers=header, params=query_params).json()
    except Exception as e:
        print("Error: ",e)
        while True:
            try:
                return requests.get(url=base_url+url_ext, headers=header, params=query_params).json()
            except Exception as e:
                print("Error: ",e)
                time.sleep(5)
                continue

def get_data(query_start:datetime.datetime,query_end:datetime.datetime,fp:str,start_time:datetime.datetime):
    resp = get_data_historical(query_start,query_end)
    if len(resp["data"]) == 0:
        resp["data"]= [
            {
                "timestamp":None,
                "price":None,
                "volume":None,
                "count":0,
                "sources":[],
            }
        ]
        df = pd.DataFrame(resp["data"])
        df["query_start_time"] = resp["query"]["start_time"]
        df["query_end_time"] = resp["query"]["end_time"]
        save_result(df,fp+"/"+str(start_time)+".json")
    else:
        df = format_result(resp)
        save_result(df,fp+"/"+str(start_time)+".json")
    
def test():
    query_start = datetime.datetime(2023, 4, 18, 10, 10, 30)
    query_end = query_start + datetime.timedelta(seconds=30)
    resp = get_data_historical(query_start,query_end)
    df = format_result(resp)
    with open("./data/kaiko/test.json","w") as f:
        for i in df.to_dict(orient="records"):
            f.write(json.dumps(i))
            f.write(",\n")
    return 

def format_result(resp:dict):
    df = pd.DataFrame(resp["data"])
    df["query_start_time"] = resp["query"]["start_time"]
    df["query_end_time"] = resp["query"]["end_time"]
    df = to_date_string(df,["timestamp"])
    return df

def save_result(df:pd.DataFrame,fp:str):
    with open(fp,"a") as f:
        for i in df.to_dict(orient="records"):
            f.write(json.dumps(i))
            f.write(",\n")
    return



def main():
    '''
    This function gets the kline data from binance api and saves it in csv files.
    there are 3 parameters that can be changed to get the data in different time intervals.
    1. chunk_duration: this is the duration of data that will be saved in 1 csv file.
    2. query_duration: this is the duration of data that will be requested in 1 request.
    3. total_duration: this is the total duration of data that will be requested.
    Lastly if your requests are denied by binance api, you can add a sleep time between each request using sleep_time parameter.
    '''
    print(query)
    end_time = query["end_time"] # the script ends data at 00:00:00 of the current day
    start_time = query["start_time"]
    chunk_duration = query["chunk_duration"] # 1 day data in 1 csv file
    query_duration =query["query_duration"] # seconds
    sleep_time = 0 # 5 seconds
    fp = "./data/kaiko/"+query["name"]
    if not os.path.exists(fp):
        os.makedirs(fp)
    print("stats:",stats(start_time,end_time,chunk_duration,query_duration))

    cnt = 0
    while start_time < end_time:
        iterations = math.floor(chunk_duration/query_duration)
        query_start = start_time
        cnt+=1
        for i in range(iterations):
            query_end = (query_start + datetime.timedelta(seconds=query_duration))   
            get_data(query_start,query_end,fp,start_time)
            query_start = query_end
            if sleep_time > 0:
                time.sleep(sleep_time)
        query_end = start_time + datetime.timedelta(seconds=chunk_duration)
        get_data(query_start,query_end,fp,start_time)
        print("chunk:", cnt,"start_time",start_time.isoformat(),"end_time",query_end.isoformat())
        start_time=start_time + datetime.timedelta(seconds=chunk_duration)



if __name__ == "__main__":
    test()