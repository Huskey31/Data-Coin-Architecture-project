import requests
import pandas as pd
from datetime import datetime
import logging
from sqlalchemy import create_engine

logging.basicConfig(
    filename= r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Coin_Crypto\coin_crypto_log.log",
    level=logging.INFO,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode='a'
)

try:
    #Extracting from api
    logging.info("Starting data extraction from CoinGecko API")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    #setting parameters 
    para_met = { "vs_currency": "usd",          
        "ids": "bitcoin,ethereum,ripple,cardano",  
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1 }
    response = requests.get(url,params=para_met)
    data = response.json()

    #appending data to table 
    table_data = []
    for coin in data:
        table_data.append({
            "Coin": coin["name"],
            "Price": coin["current_price"],
            "Market Cap": coin["market_cap"]
        })
    logging.info("Data extraction completed successfully")

    # Changing to dataframe
    logging.info("Transforming data into DataFrame")
    df = pd.DataFrame(table_data)
    #Changing price and market cap to integer
    df["Price"] = pd.to_numeric(df["Price"])
    df["Market Cap"] = pd.to_numeric(df["Market Cap"])
    df = df.drop_duplicates()
    logging.info("Data transformation completed successfully")  

    #loading to database
    logging.info("Loading data into PostgreSQL database")
    engine = create_engine("postgresql+psycopg2://######:#######@Localhost:5432/ETL_Database") #Changed code to hide credentials
    df.to_sql("coin_crypto_data", engine, if_exists='append', index=False)
    logging.info("Data loaded to database successfully")
    print(df.head())
except Exception as e:
    logging.error("An error occurred  at: {e}")