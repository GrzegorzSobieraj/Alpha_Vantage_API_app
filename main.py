from datetime import datetime
from http.client import RemoteDisconnected
import logging
import multiprocessing
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
import pandas
import pyarrow
import pyarrow.parquet
import requests
import time


def log(function):
    def inside(*args, **kwargs):
        start = datetime.today()
        current_time = start.strftime('%Y_%m_%d_%H_%M_%S_%f')
        logging.basicConfig(filename=f'{current_time}.csv', encoding='utf-8', force=True, level=logging.DEBUG)
        status = function(*args, **kwargs)
        end = datetime.today()
        process_name = multiprocessing.current_process().name
        execution_time = end - start
        if status == 'success':
            logging.debug(
                f'process name:{process_name}, parameter:{args},execution time:{execution_time},status:{status}')
        else:
            logging.error(
                f'process name:{process_name},parameter:{args},execution time:{execution_time},status:{status}')
    return inside


def download(function):
    tickers = [
        'AAL', 'ACN', 'ADBE', 'AMZN', 'APLE', 'BAC', 'BK', 'CAG', 'CATY', 'CHN', 'DPZ', 'EYES', 'GOOGL', 'IBM', 'JVA',
        'KHC', 'KO', 'MCD', 'MSI', 'PEP', 'RNST', 'SBUX', 'TSLA', 'YUM', 'XXX'
    ]
    for t in tickers:
        function(t)


@log
def get_ema(ticker):
    if type(ticker) != str or len(ticker) < 1:
        return 'error'
    ticker = ticker.upper()
    while True:
        try:
            url = f'https://www.alphavantage.co/query?function=EMA&symbol={ticker}&interval=daily&time_period=77' \
                  f'&series_type=close&apikey=JTTL84KYIN40MUZ8'
        except (ConnectionError, ProtocolError, RemoteDisconnected):
            continue
        else:
            break
    data = requests.get(url).json()
    if data == {}:
        return 'error'
    while 'Note' in data.keys():
        time.sleep(60)
        data = requests.get(url).json()
    else:
        data_frame = pandas.DataFrame(data, index=list(d for d in range(len(data))))
        table = pyarrow.Table.from_pandas(data_frame)
        current_time = datetime.today().strftime('%Y_%m_%d_%H_%M_%S_%f')
        pyarrow.parquet.write_table(table, f'{current_time}.parquet', compression='SNAPPY')
        return 'success'


if __name__ == '__main__':
    download(get_ema)
