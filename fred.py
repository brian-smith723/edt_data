import psycopg2 as pg  
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
from base import _BaseWriter
from config import CONN_FRED 
from edt_datareader.data import DataReader
from client import create_logging_connection, PerfLoggingCursor
from pandas import DataFrame
import pandas as pd

class FredWriter(_BaseWriter):
    """
    Write data for the given name from the St. Louis FED (FRED) to database.
    Date format is datetime

    """

    def __init__(self): 
        if self.frequency == "daily":
            self.logger.info("performing daily cron")
        if self.frequency == "weekly":
            self.logger.info("performing weekly cron")
        if self.frequency == "monthly":
            self.logger.info("performing monthly cron")

        self.connection = CONN_FRED 

    def write(self):
        self.logger.info("starting to write...")
        for record in self.records:
           df = DataReader(record[0], "fred")
           conn = create_logging_connection(self.connection)
           cur = conn.cursor(cursor_factory=PerfLoggingCursor)
           for row in df.index.get_values():
               table_name = df.columns.values.tolist()[0]
               date = row.astype('M8[D]').astype('O') # parses 2009-12-31T19:00:00.000000000-0500 to 2009-12-31
               value = df.get_value(row, table_name)
               query =  """INSERT INTO """+ str(table_name) + """(date, value) 
               VALUES (%s, %s) ON CONFLICT DO NOTHING;"""
               data = (date, value)
               try:
                   cur.execute(query, data)
               except (pg.ProgrammingError, pg.IntegrityError) as e:
                   self.logger.error(e.pgerror)
                   conn.rollback()

           conn.commit()
           cur.close()
        self.logger.info("finished writing...")
