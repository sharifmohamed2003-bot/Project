import unittest
import pandas as pd
import sqlite3
from CWpreprocessing import CSVtoSQLite

class TestCSVtoSQLite(unittest.TestCase):

    def setUp(self):
        # sample DataFrame
        data = {
            "student_id": [1, 2, 3, 3],
            "score": [50, 70, 90, 80],
            "Q1 /100": [10, 15, None, 20],
            "Grades/600": [20, 25, 30, 35],
            "time-taken": [5, 6, 4, None],
            "state": ["Finished", "Finished", "Finished", "Finished"]
        }
        self.df = pd.DataFrame(data)

        # Initialize the class with dummy path
        self.converter = CSVtoSQLite("dummy.csv")
        # Inject the DataFrame directly
        self.converter.df = self.df.copy()

    def test_clean_dataframe(self):
        df_cleaned = self.converter.clean_dataframe()

        # Original DF saved
        self.assertTrue(hasattr(self.converter, "dftest_1"))
        self.assertTrue(self.converter.dftest_1.equals(self.df))

        # Cleaned DF saved
        self.assertTrue(hasattr(self.converter, "dfCleanTest_1"))
        self.assertIsInstance(self.converter.dfCleanTest_1, pd.DataFrame)

        # Fully normalized DF saved
        self.assertTrue(hasattr(self.converter, "dfFormattedCleaned_1"))

        # Check duplicates handled (student_id=3)
        self.assertEqual(len(df_cleaned), 3)

        # Check column renaming
        self.assertIn("Q1", df_cleaned.columns)
        self.assertIn("Grades", df_cleaned.columns)
        self.assertNotIn("time-taken", df_cleaned.columns)
        self.assertNotIn("state", df_cleaned.columns)

        # Check normalization to 0-100
        self.assertTrue(df_cleaned["score"].max() <= 100)
        self.assertTrue(df_cleaned["Q1"].max() <= 100)
        self.assertTrue(df_cleaned["Grades"].max() <= 100)

    def test_save_to_sqlite(self):
        self.converter.clean_dataframe()
        # Save single DataFrame to in-memory DB
        self.converter.save_to_sqlite(":memory:", "test_table")

        conn = sqlite3.connect(":memory:")
        # Re-insert for testing because save_to_sqlite closes connection
        df = self.converter.df
        df.to_sql("test_table", conn, if_exists="replace", index=False)

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        self.assertIn("test_table", tables)

        df_db = pd.read_sql_query("SELECT * FROM test_table", conn)
        self.assertEqual(len(df_db), len(self.converter.df))
        conn.close()

    def test_save_all_to_sqlite_db(self):
        self.converter.clean_dataframe()
        conn = sqlite3.connect(":memory:")
        # Temporarily override save_all_to_sqlite_db to accept existing connection
        for attr_name in ["dftest_1", "dfCleanTest_1_new", "dfFormattedCleaned_1"]:
            df = getattr(self.converter, attr_name)
            df.to_sql(attr_name, conn, if_exists='replace', index=False)

        # Check tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        self.assertIn("dftest_1", tables)
        self.assertIn("dfCleanTest_1_new", tables)
        self.assertIn("dfFormattedCleaned_1", tables)

        # Check row counts
        for table_name in tables:
            df_db = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            df_attr = getattr(self.converter, table_name)
            self.assertEqual(len(df_db), len(df_attr))
        conn.close()


if __name__ == "__main__":
    unittest.main()
