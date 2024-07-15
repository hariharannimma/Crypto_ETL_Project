import requests
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO


def cryptoExtract():
    to_time = int(time.time())
    from_time = to_time - 900
    singleBitcoinUrl = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from={from_time}&to={to_time}"

    headers = {"accept": "application/json"}

    singleBitcoinresponse = requests.get(singleBitcoinUrl, headers=headers)
    data = json.loads(singleBitcoinresponse.text)
    
    prices_data = data["prices"]
    market_cap = data["market_caps"]
    total_volume = data["total_volumes"]

    timestamps = [data[0] for data in prices_data]
    prices = [data[1] for data in prices_data]
    market_caps = [data[1] for data in market_cap]
    total_volumes = [data[1] for data in total_volume]
    
    bitcoinData = {
        "time": timestamps,
        "prices": prices,
        "market_cap": market_caps,
        "total_volume": total_volumes
    }
    
    bitcoinDf = pd.DataFrame(bitcoinData)
    
    bitcoinDf['time'] = pd.to_datetime(bitcoinDf['time'], unit='ms')
    
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    csv_buffer = StringIO()
    bitcoinDf.to_csv(csv_buffer, index=False)
    current_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"bitcon-data/bitcoin_{current_timestamp}.csv"
    s3_hook.load_string(string_data=csv_buffer.getvalue(), key=filename, bucket_name='crypto-bitcoin-dev-uswest1', replace=True)
    
    
with DAG(
    dag_id = "crypto_bitcoin_to_s3",
    start_date = datetime(2024, 7, 14),
    schedule="*/15 * * * *",
    catchup = False,
    tags=["Bitcoin"],
) as dag:
    
    crypto_bitcoin_to_s3 = PythonOperator(
        task_id = "crypto_bitcoin_to_s3",
        python_callable = cryptoExtract
    )