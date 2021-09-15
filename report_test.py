import unittest
from report import INSERT_YEAR, INSERT_PRODUCT, INSERT_COUNTRY, INSERT_SALES 

class InsertQueryTests(unittest.TestCase):

    def insertYearString(self):
        """Checks the Insert Year query"""
        self.assertEqual(INSERT_YEAR(2020), 'INSERT INTO YEAR(year) VALUES(2020);')


if __name__ == "__main__":
    unittest.main()

