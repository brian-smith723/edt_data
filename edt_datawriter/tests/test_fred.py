import nose
import pandas.util.testing as tm
from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal
from edt_datawriter.fred import FredWriter
from edt_datawriter.client import get_fred_logger
from edt_datawriter.config import CONN_COMMON, CONN_FRED 

fred_logger = get_fred_logger()

class TestFred(tm.TestCase):
    def test_fred_failed_series(self):
        #Arrange
        records =['series', 'indicator', 'data']
        expected = []
        #Act
        updated_records = FredWriter(records, frequency='daily',logger=fred_logger,
                              connection=CONN_FRED).write()
        #Assert
        self.assertEqual(expected, updated_records)

    def test_fred_failed_database(self):
        #Arrange
        records =['GDP', 'CPI']
        expected = []
        #Act
        updated_records = FredWriter(records, frequency='daily',logger=fred_logger,
                              connection=CONN_FRED).write()
        #Assert
        self.assertEqual(expected, updated_records)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x'])# '--pdb', '--pdb-failure'],
                   #exit=False)
