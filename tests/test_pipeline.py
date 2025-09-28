import unittest
import pandas as pd
import inspect
import os
import sys

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from pipeline import clean_chunk


class TestPipeline(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self._df = pd.DataFrame({
            "quantity": ["10", "abc", "-5"],
            "unit_price": [10, 1000, 20],
            "discount_percent": [0.5, 1.0, -0.2],
            "region": ["nort", "Sth", "North"],
            "category": ["misc", "misc", "misc"],
            "product_name": ["x", "y", "z"],
            "sale_date": ["2025-09-28","2024-08-12", "2024-01-03"],
            "customer_email": ["a@test.com", "b@test.com", "c@test.com"],
        })

    def test_clean_quantity(self):
        cleaned = clean_chunk(self._df)
        self.assertEqual(cleaned["quantity"].tolist(), [10, 0])

    def test_clean_discount(self):
        cleaned = clean_chunk(self._df)
        self.assertTrue((cleaned["discount_percent"] >= 0).all() and (cleaned["discount_percent"] <= 1).all())

    def test_clean_region(self):
        cleaned = clean_chunk(self._df)
        self.assertTrue(set(cleaned["region"].unique()) <= {"north", "south"})

    def test_revenue_calculation(self):
        cleaned = clean_chunk(self._df)
        self.assertEqual(cleaned["revenue"].iloc[0], 10*10*(1-0.5))

    def test_month_extraction(self):
        cleaned = clean_chunk(self._df)
        self.assertEqual(cleaned["month"].tolist(), ["2025-09", "2024-08"])


if __name__ == "__main__":
    unittest.main()
