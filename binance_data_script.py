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

def main():
    
    total_duration = 30*24*60*60 # 30 days
    end_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) 
    start_time = end_time - datetime.timedelta(seconds=total_duration)
    chunk_duration = 24*60*60 # 1 day data in 1 csv file
    query_duration =1000 # 1 hour data in 1 request
    sleep_time = 5 # 5 seconds
    print("stats:",stats(start_time,end_time,chunk_duration,query_duration))

    df_cols = ["openTime","openPrice","highPrice","lowPrice","closePrice","volume","closeTime","quoteAssetVolume","numberOfTrades","takerBuyBaseAssetVolume","takerBuyQuoteAssetVolume","ignore"]
    cnt = 0
    while start_time < end_time:
        temp_df = pd.DataFrame(columns=df_cols)
        iterations = math.ceil(chunk_duration/query_duration)
        query_start = start_time
        cnt+=1
        for i in range(iterations):
            query_end = (query_start + datetime.timedelta(seconds=query_duration))   
            query_params = {
            "symbol":"BTCUSDT", 
            "startTime":int(query_start.timestamp()*1000),
            "endTime":int(query_end.timestamp()*1000),
            "interval":"1s", 
            "limit":1000
            }
            resp = requests.get(url=base_url+url_ext, params=query_params)
            temp_df = pd.concat([temp_df,pd.DataFrame(resp.json(),columns=df_cols)],axis=0)
            query_start = query_end
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        temp_df["openTime"] = pd.to_datetime(temp_df['openTime'],dayfirst=True,unit='ms')
        temp_df["closeTime"] = pd.to_datetime(temp_df['closeTime'],dayfirst=True,unit='ms')
        temp_df.to_csv("data/"+str(start_time.isoformat())+".csv",index=False)
        print("chunk:", cnt,"start_time",start_time.isoformat(),"end_time",query_start.isoformat())
        start_time=start_time + datetime.timedelta(seconds=query_duration*iterations)

if __name__ == "__main__":
    main()