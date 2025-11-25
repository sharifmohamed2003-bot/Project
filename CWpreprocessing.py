"""
CSV to SQLite Converter
#################################################
This module defines a class that can:
1. Load a CSV into a pandas DataFrame
2. Format/clean the DataFrame
3. Store the DataFrame into a SQLite database

Use this module in Jupyter Notebooks by importing the class.
"""
import re
import pandas as pd
import sqlite3
from pathlib import Path


class CSVtoSQLite:
    """
    A class for loading CSV files, formatting DataFrames,
    and saving them into SQLite databases.
    """

    _clean_counter = 0   # internal tracker for dfCleanTest_(num)

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.df = None

    def load_csv(self) -> pd.DataFrame:
        """Load a CSV file into a pandas DataFrame."""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        self.df = pd.read_csv(self.csv_path)
        return self.df

    def normalize_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize numeric score values to be out of 100."""

        score_cols = [
            col for col in df.columns
            if re.match(r"^Q\d+$", col)
            or re.match(r"^Grades\d+$", col)
            or col == "score"
        ]

        for col in score_cols:
            max_val = df[col].max()
            if max_val > 0:
                df[col] = (df[col] / max_val) * 100
            else:
                df[col] = 0

        return df

    def clean_dataframe(self) -> pd.DataFrame:
        """
        Clean DataFrame:
        - Rename columns (remove spaces, '/', '?')
        - Replace nulls with 0
        - Drop redundant columns
        - Keep only highest score per student if duplicates exist
        """

        if self.df is None:
            raise ValueError("DataFrame is not loaded. Call load_csv() first.")

        CSVtoSQLite._clean_counter += 1
        test_num = CSVtoSQLite._clean_counter

        # Save the original DataFrame before cleaning
        old_name = f"dftest_{test_num}"
        setattr(self, old_name, self.df.copy())

        df = self.df.copy()

        # Clean column names
        clean_cols = []
        for col in df.columns:
            col_clean = col.strip()
            col_clean = re.sub(r"[ /?]", "", col_clean)
            col_clean = re.sub(r"Q(\d+).*", r"Q\1", col_clean)
            col_clean = re.sub(r"Grades(\d+).*", r"Grades\1", col_clean)
            clean_cols.append(col_clean)
        df.columns = clean_cols

        # Replace nulls with 0
        df = df.fillna(0)

        # Drop redundant columns
        for col in ['state', 'time-taken']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # Keep only highest score per student
        if 'student_id' in df.columns and 'score' in df.columns:
            df = df.sort_values('score', ascending=False).drop_duplicates(subset=['student_id'])

        # Save the cleaned but not yet normalized DataFrame
        clean_name = f"dfCleanTest_{test_num}"
        setattr(self, clean_name, df.copy())

        # Normalize score columns to out of 100
        df = self.normalize_scores(df)

        # Save the fully formatted & cleaned DataFrame
        formatted_name = f"dfFormattedCleaned_{test_num}"
        setattr(self, formatted_name, df.copy())

        self.df = df
        return df

    def save_to_sqlite(self, db_path: str, table_name: str) -> None:
        """Save the DataFrame into a SQLite database table."""
        if self.df is None:
            raise ValueError("DataFrame is not ready. Load and format the CSV first.")

        db_path = Path(db_path)
        conn = sqlite3.connect(db_path)
        self.df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()

    def convert(self, db_path: str, table_name: str) -> None:
        """
        Complete process:
        - Load CSV
        - Format DataFrame
        - Save to SQLite
        """
        self.load_csv()
        self.clean_dataframe()
        self.save_to_sqlite(db_path, table_name)



# Example Jupyter Notebook usage:
# --------------------------------------------------
# from csv_to_sqlite_module import CSVtoSQLite
# converter = CSVtoSQLite("input.csv")
# converter.convert("database.db", "my_table")
