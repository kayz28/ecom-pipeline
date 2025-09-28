import unittest
import pandas as pd
from pipeline import clean_chunk

class TestPipeline(unittest.TestCase):

    def test_clean_quantity(self):
        df = pd.DataFrame({"quantity": ["10", "abc", "-5"]})
        cleaned = clean_chunk(df)
        self.assertEqual(cleaned["quantity"].tolist(), [10, 0, -5])

    def test_clean_discount(self):
        df = pd.DataFrame({"discount_percent": [0.5, 1.5, -0.2]})
        cleaned = clean_chunk(df)
        self.assertTrue((cleaned["discount_percent"] >= 0).all() and (cleaned["discount_percent"] <= 1).all())

    def test_clean_region(self):
        df = pd.DataFrame({"region": ["nort", "Sth", "North"]})
        cleaned = clean_chunk(df)
        self.assertTrue(set(cleaned["region"].unique()) <= {"north", "south"})

    def test_revenue_calculation(self):
        df = pd.DataFrame({"quantity": [2], "unit_price": [100], "discount_percent": [0.1]})
        cleaned = clean_chunk(df)
        self.assertEqual(cleaned["revenue"].iloc[0], 2*100*(1-0.1))

    def test_month_extraction(self):
        df = pd.DataFrame({"sale_date": ["2025-09-28", "2025/08/15"]})
        cleaned = clean_chunk(df)
        self.assertEqual(cleaned["month"].tolist(), ["2025-09", "2025-08"])


if __name__ == "__main__":
    unittest.main()
