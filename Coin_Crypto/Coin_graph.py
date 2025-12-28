import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt
import logging
from datetime import datetime
import os

logging.basicConfig(
    filename= r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Coin_Crypto\coin_crypto_log.log",
    level=logging.INFO,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode='a'
)

try:
    logging.info("Starting the Coin Chart Pipeline")
    df = pd.read_sql(
        "SELECT * FROM coin_crypto_data",
        sqlalchemy.create_engine("postgresql+psycopg2://######:######@Localhost:5432/ETL_Database") #Changed code to hide credentials
    )

    plt.figure(figsize=(10,6))
    # Scatter each price column against Volume
    plt.scatter(df["Coin"], df["Price"], color='blue', alpha=0.5, label='Price')
    plt.scatter(df["Coin"], df["Market Cap"], color='green', alpha=0.5, label='Market Cap')
    

    plt.title("Coin Gecko Crypto Data Scatter Plot")
    plt.xlabel("Coin")
    plt.ylabel("Values")
    plt.grid(True)
    plt.legend()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Coin_Crypto\Graph"
    os.makedirs(save_path, exist_ok=True)
    filename = os.path.join(save_path, f"yfinance_{timestamp}.png") 

    plt.savefig(filename, dpi=300)
    logging.info("Coin chart successfully loaded")
except Exception as e:
    logging.error(f"Error as occurred {e}")