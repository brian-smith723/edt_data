import psycopg2 as pg  
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
from .base import _BaseWriter
from edt_datareader.data import DataReader
from .client import create_logging_connection, PerfLoggingCursor
from pandas import DataFrame
import pandas as pd
from edt_datawriter.config import CONN_COMMON, GET_FRED_DAILY, GET_FRED_WEEKLY, GET_FRED_MONTHLY, CONN_FRED, UPDATE_DATE,GET_FRED_BIWEEKLY, GET_FRED_MONTHLY, GET_FRED_QUARTERLY, GET_FRED_SEMIANNUAL, GET_FRED_ANNUAL, GET_FRED_5YEAR


data_source = { 
            'fred':{
                'daily':GET_FRED_DAILY,
                'weekly':GET_FRED_WEEKLY,
                'biweekly':GET_FRED_BIWEEKLY,
                'monthly':GET_FRED_MONTHLY,
                'quarterly':GET_FRED_QUARTERLY,
                'semiannual':GET_FRED_SEMIANNUAL,
                'annual':GET_FRED_ANNUAL,
                '5year':GET_FRED_5YEAR,
                'update':UPDATE_DATE,
                   },
            }

class CommonWriter(_BaseWriter):
    """
    Write data to common database.
    """
    def failed(self):
        """ Returns a list of indicators that failed to update"""
        return self.failed 

    def connect(self):
        """ Creates connection to specified database """
        return create_logging_connection(self.connection)

    def cursor(self, conn):
        return conn.cursor(cursor_factory=PerfLoggingCursor)

    def which_proc(self):
        """ Returns stored procedure"""
        if self.frequency == "daily":
            return data_source[self.data_source]['daily'] 
        if self.frequency == "weekly":
            return data_source[self.data_source]['weekly'] 
        if self.frequency == "biweekly":
            return data_source[self.data_source]['biweekly'] 
        if self.frequency == "monthly":
            return data_source[self.data_source]['monthly'] 
        if self.frequency == "quarterly":
            return data_source[self.data_source]['quarterly'] 
        if self.frequency == "semiannual":
            return data_source[self.data_source]['semiannual'] 
        if self.frequency == "annual":
            return data_source[self.data_source]['annual'] 
        if self.frequency == "5year":
            return data_source[self.data_source]['5year'] 
        if self.frequency =="update":
            return data_source[self.data_source]['update'] 

    def get_records(self):
        conn = self.connect()#create_logging_connection(self.connection)
        cur =  self.cursor(conn)#conn.cursor(cursor_factory=PerfLoggingCursor)
        cur.callproc(self.which_proc()) 
        records = cur.fetchall()
        cur.close()
        return records

    def update(self):
        self.logger.info("starting to update...")
        for record in self.records: 
            try:
                conn = self.connect()
                cur = self.cursor(conn)
                cur.callproc(self.which_proc(),(record[0],)) 
                conn.commit()
            except (Exception) as e:
                self.logger.error("Common error {}".format(e))
        cur.close()
        self.logger.info("finished updaing...")
        return  
