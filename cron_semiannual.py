from edt_datareader.data import DataReader
from pandas import DataFrame
import pandas as pd
from edt_datawriter.client import get_fred_logger, create_logging_connection, PerfLoggingCursor
from edt_datawriter.config import CONN_COMMON, GET_FRED_DAILY, CONN_FRED, UPDATE_DATE
from edt_datawriter.fred import FredWriter
from edt_datawriter.common import CommonWriter 

#Database loggers
fred_logger = get_fred_logger()

def update_semmiannually_indicators():
    """
    Writes daily data to database from a number of online sources.

    Currently supports St. Louis FED (FRED)
    """
    data_sources = [
            'fred',
            ]

    for data_source in data_sources:
        if data_source == "yahoo":
            raise NotImplementedError
        if data_source == 'fred':
            records = CommonWriter(data_source='fred',frequency='semiannual',logger=fred_logger,
                                   connection=CONN_COMMON).get_records()
            updated_fred_records = FredWriter(records, frequency='semiannual',logger=fred_logger,
                              connection=CONN_FRED).write()
            CommonWriter(data_source='fred',records=records, frequency='update',logger=fred_logger, 
                        connection=CONN_COMMON).update()
if __name__ == "__main__":
    update_semmiannually_indicators()
