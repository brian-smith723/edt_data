import nose
import pandas.util.testing as tm
from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal

class TestFred(tm.TestCase):
    def test_fred(self):
        fred = FredWriter()


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x'])# '--pdb', '--pdb-failure'],
                   #exit=False)
