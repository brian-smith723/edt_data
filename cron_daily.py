import psycopg2 as pg  
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
from edt_datareader.data import DataReader
from pandas import DataFrame
import pandas as pd
from client import get_fred_logger, create_logging_connection, PerfLoggingCursor
from config import CONN_COMMON, GET_FRED_DAILY, CONN_FRED
from fred import FredWriter

#Database loggers
fred_logger = get_fred_logger()

def perform_daily_cron():
    """
    Writes daily data to database from a number of online sources.

    Currently supports St. Louis FED (FRED)
    """
    data_sources = [
            'fred',
            ]

    conn = create_logging_connection(CONN_COMMON)
    cur = conn.cursor(cursor_factory=PerfLoggingCursor)

    for data_source in data_sources:
        if data_source == "yahoo":
            raise NotImplementedError
        if data_source == 'fred':
            cur.callproc(GET_FRED_DAILY) 
            records = cur.fetchall()
            cur.close()
            FredWriter(records, frequency='daily',logger=fred_logger,
                    connection=CONN_FRED).write()
        
if __name__ == "__main__":
    perform_daily_cron()
