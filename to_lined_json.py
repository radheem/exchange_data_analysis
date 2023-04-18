import pandas as pd
from os import listdir
from os.path import isfile, join

def main():
    mypath = "./data/binance/"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 

    for i in onlyfiles:
        df = pd.read_json(mypath+i)
        df.to_json(mypath+i,orient="records",lines=True)
    
    return 


if __name__ == "__main__":
    main()