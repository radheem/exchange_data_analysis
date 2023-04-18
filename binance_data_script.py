import pandas as pd
import requests 
import datetime
import math
import time

base_url = "https://api.binance.com"
url_ext = "/api/v3/klines"

def stats(start_time,end_time,chunk_duration,query_duration):
    result = {
        "total_duration":end_time.timestamp() - start_time.timestamp(),
        "chunk_iterations":math.ceil((end_time.timestamp() - start_time.timestamp())/chunk_duration),
        "query_iterations":math.ceil(chunk_duration/query_duration),
    }
    result["total_iterations"] = result["chunk_iterations"] * result["query_iterations"]

    return result 

def get_data(query_start:datetime.datetime,query_end:datetime.datetime, symbol:str, interval:int, limit:int):
    query_params = {
        "symbol":symbol, # kline symbol
        "startTime":int(query_start.timestamp()*1000), # start time in milliseconds
        "endTime":int(query_end.timestamp()*1000), # end time in milliseconds
        "interval":str(interval)+"s",  # kline interval (find options here: https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions)
        "limit":limit # default 500 max 1000
        }
    return requests.get(url=base_url+url_ext, params=query_params)


def main():
    '''
    This function gets the kline data from binance api and saves it in csv files.
    there are 3 parameters that can be changed to get the data in different time intervals.
    1. chunk_duration: this is the duration of data that will be saved in 1 csv file.
    2. query_duration: this is the duration of data that will be requested in 1 request.
    3. total_duration: this is the total duration of data that will be requested.
    Lastly if your requests are denied by binance api, you can add a sleep time between each request using sleep_time parameter.
    '''
    symbol = "BTCUSDT"
    total_duration = 30*24*60*60 # 30 days
    end_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) # the script ends data at 00:00:00 of the current day
    start_time = end_time - datetime.timedelta(seconds=total_duration)
    chunk_duration = 24*60*60 # 1 day data in 1 csv file
    query_duration =1000 # 1 hour data in 1 request
    sleep_time = 0 # 5 seconds
    print("stats:",stats(start_time,end_time,chunk_duration,query_duration))

    df_cols = ["openTime","openPrice","highPrice","lowPrice","closePrice","volume","closeTime","quoteAssetVolume","numberOfTrades","takerBuyBaseAssetVolume","takerBuyQuoteAssetVolume","ignore"]
    cnt = 0
    while start_time < end_time:
        temp_df = pd.DataFrame(columns=df_cols)
        iterations = math.floor(chunk_duration/query_duration)
        query_start = start_time
        cnt+=1
        for i in range(iterations):
            query_end = (query_start + datetime.timedelta(seconds=query_duration))   
            resp = get_data(query_start,query_end,symbol,1,1000)
            temp_df = pd.concat([temp_df,pd.DataFrame(resp.json(),columns=df_cols)],axis=0)
            query_start = query_end
            if sleep_time > 0:
                time.sleep(sleep_time)
        query_end = start_time + datetime.timedelta(seconds=chunk_duration)
        resp = get_data(query_start,query_end,symbol,1,1000)
        temp_df = pd.concat([temp_df,pd.DataFrame(resp.json(),columns=df_cols)],axis=0)
        temp_df["openTime"] = pd.to_datetime(temp_df['openTime'],dayfirst=True,unit='ms',utc=True).astype(str)
        temp_df["closeTime"] = pd.to_datetime(temp_df['closeTime'],dayfirst=True,unit='ms',utc=True).astype(str)

        print(temp_df.iloc[0])
        temp_df.to_json("data/binance/"+str(start_time.isoformat())+".json",orient="records",lines=True)
        print("chunk:", cnt,"start_time",start_time.isoformat(),"end_time",query_end.isoformat())
        start_time=start_time + datetime.timedelta(seconds=chunk_duration)

if __name__ == "__main__":
    main()