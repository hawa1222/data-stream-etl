import unittest
import pandas as pd

from utility.utils import convert_to_time

class TestConvertToTime(unittest.TestCase):

    def test_convert_to_time(self):
        self.assertEqual(convert_to_time(4.89), "4:53")
        self.assertEqual(convert_to_time(0), None)
        self.assertEqual(convert_to_time(10.5), "10:30")
        self.assertEqual(convert_to_time(1.01), "1:01")
        self.assertEqual(convert_to_time(5.1), "5:06")
        self.assertEqual(convert_to_time(None), None)
        self.assertEqual(convert_to_time(pd.NA), None)

if __name__ == '__main__':
    unittest.main()
