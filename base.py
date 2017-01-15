from pandas import DataFrame

class _BaseWriter(object):
    """
    Base class
    """
    def __init__(self, records, frequency=None, logger=None, connection=None, proc=None):

        if len(records) == 0: 
            raise ValueError("We have no records. Please check the database") 
        if connection == None: 
            raise ValueError("Connection cannot be None.") 
        if logger == None: 
            raise ValueError("Logger cannot be None.") 
        if frequency == None: 
            raise ValueError("Frequency cannot be None.") 

        self.records = records
        self.frequency = frequency
        self.logger = logger
        self.connection = connection
        self.proc = proc
        self.failed = []

        if self.frequency == "daily":
            self.logger.info("performing daily cron")
        if self.frequency == "weekly":
            self.logger.info("performing weekly cron")
        if self.frequency == "monthly":
            self.logger.info("performing monthly cron")

    @property
    def get_failed_indicators(self):
        # must be overridden in subclass
        raise NotImplementedError

    
