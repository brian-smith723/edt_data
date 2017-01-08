from pandas import DataFrame

class _BaseWriter(object):
    """
    Base class
    """
    def __init__(self, records, frequency=None, logger=None, connection=None, proc=None):
        if len(records) == 0: 
            raise ValueError("We have no records. Please check the database") 
        self.records = records
        self.frequency = frequency
        self.logger = logger
        self.connection = connection
        self.proc = proc

    #@property
    #def connection(self):
    #    #must be overridden in subclass
    #    raise NotImplementedError

