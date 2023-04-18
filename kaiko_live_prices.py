import datetime
import pandas as pd
import requests 
import json
import time
import os
from dotenv import load_dotenv
import math


load_dotenv()
kaiko_symbol = "btc/usdt"
base_url = "https://us.market-api.kaiko.io/v2/data/trades.v1/"
url_ext = "spot_direct_exchange_rate/" + kaiko_symbol

api_key = os.getenv('KAIKO_APIKEY')
query1 = {
    "name":"current_query_no_start_end",
    "include_exchanges": [
      "binc",
      "cbse",
      "okex",
      "huob",
      "hitb",
      "krkn",
      "kcon",
    ], 
    "sources":True,
    "interval":"10s",
    "sort":"desc",
    "page_size":2,
    "chunk_size":5000,
    "elapse":1
}
query2 = {
    "name":"current_query_no_start_end",
    "include_exchanges": [
      "binc",
      "cbse",
      "okex",
      "huob",
      "hitb",
      "krkn",
      "kcon",
    ], 
    "sources":True,
    "interval":"10s",
    "sort":"desc",
    "page_size":2,
    "chunk_size":5000,
    "elapse":10
}
query = query1
def to_date_string(df:pd.DataFrame,columns):
    if isinstance(columns,str):
        columns = [columns]
    for column in columns:
        df[column] = pd.to_datetime(df[column],dayfirst=True,unit='ms',utc=True).astype(str)
    return df


def format_result(resp:dict,curr_time:datetime.datetime):
    df = pd.DataFrame(resp["data"])
    df["query_time"] = curr_time*1000
    df = to_date_string(df,["timestamp","query_time"])
    return df

def save_result(df:pd.DataFrame,fp:str):
    with open(fp,"a") as f:
        for i in df.to_dict(orient="records"):
            f.write(json.dumps(i))
            f.write(",\n")
    return

def get_data_latest():
    header = {
        "X-Api-Key": api_key,
        "timeout": str(10*1000),

    }
    query_params = {
        "include_exchanges": ','.join(query["include_exchanges"]),
        "sources": query["sources"],
        "interval": query["interval"],
        "sort": query["sort"],
        "page_size": query["page_size"]
    }
    try:
        return requests.get(url=base_url+url_ext, headers=header, params=query_params).json()
    except Exception as e:
        print("Error: ",e)
        cnt = 0
        while cnt<10:
            try:
                return requests.get(url=base_url+url_ext, headers=header, params=query_params).json()
            except Exception as e:
                print("Error: ",e)
                time.sleep(5)
            cnt+=1

def main():
    fp = "./data/kaiko/"+query["name"]
    chunk_size = query["chunk_size"]
    elapse = query["elapse"]
    if not os.path.exists(fp):
        os.makedirs(fp)
    cnt = 0
    start = datetime.datetime.now().timestamp()
    while True:
        now = datetime.datetime.now().timestamp()
        if start + elapse*cnt < now:
            data = get_data_latest()
            df = format_result(data,now)
            ext = "/live_{}_{}.json".format(elapse,query["page_size"])
            save_result(df,fp+ext)
            cnt+=1
        else:
            continue

if __name__ == "__main__":
    main()