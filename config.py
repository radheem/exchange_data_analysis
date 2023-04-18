import datetime

query1 = {
    "name":"current_query",
    "include_exchanges": [
      "bnce",
      "cbse",
      "okex",
      "huob",
      "hitb",
      "krkn",
      "kcon",
    ], 
    "sources":True,
    "query_duration":1000,
    "chunk_duration":60*60,
    "start_time":datetime.datetime(2023,3,14,0,0,0),
    "end_time":datetime.datetime(2023,4,13,0,0,0),
    "interval":"10s",
    "sort":"asc",
    "page_size":1000
}
query_binc = {
    "name":"current_query_no_exchange_list",
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
    "query_duration":1000,
    "chunk_duration":24*60*60,
    "start_time":datetime.datetime(2023,3,14,0,0,0),
    "end_time":datetime.datetime(2023,4,13,0,0,0),
    "interval":"10s",
    "sort":"asc",
    "page_size":1000
}
query = query_binc