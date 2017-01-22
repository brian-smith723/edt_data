import logging, logging.handlers
import psycopg2 
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import time

rootLogger = logging.getLogger('')
rootLogger.setLevel(logging.DEBUG)

socketHandler = logging.handlers.SocketHandler('wksp003',
                    logging.handlers.DEFAULT_TCP_LOGGING_PORT)

rootLogger.addHandler(socketHandler)
logger_pg = logging.getLogger('pg')
logger_fred = logging.getLogger('fred')
logger_wb = logging.getLogger('wb')

def get_fred_logger():
    return logger_fred

def get_wb_logger():
    return logger_wb

class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        logger_pg.info(self.mogrify(sql, args))
        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as exc:
            logger_pg.error("%s: %s" % (exc.__class__.__name__, exc))
            raise
    
    def callproc(self, sql, args=None):
        logger_pg.info(self.mogrify(sql, args))
        try:
            psycopg2.extensions.cursor.callproc(self, sql, args)
        except Exception as exc:
            logger_pg.error("%s: %s" % (exc.__class__.__name__, exc))
            raise

class PerfLoggingConnection(LoggingConnection):
    def initialize(self, logobj, mintime=0):
        LoggingConnection.initialize(self, logobj)

    def filter(self, msg, curs):
        elapsed = (time.time() - curs.timestamp) * 1000
        request = threadlocal.get_current_request()
        request_id = getattr(request, 'id', '-') if request else '-' 
        cleaned_msg = ' '.join([l.strip() for l in msg[:30].splitlines()]) 

        return "query=\"{0}\" service={1}ms rid={2}".format(cleaned_msg, int(elapsed), request_id)

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', PerfLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)

class PerfLoggingCursor(LoggingCursor):
    def execute(self, sql, args=None):
        self.timestamp = time.time()
        return LoggingCursor.execute(self, sql, args)

    def callproc(self, procname, args=None):
        self.timestamp = time.time()
        return LoggingCursor.callproc(self, procname, args)

def create_logging_connection(dsn, db_logger=None):
    conn = PerfLoggingConnection(dsn)
    conn.initialize(logger_pg)
    return conn
