"""
CSV to SQLite Converter
#################################################
This module defines a class that can:
1. Load a CSV into a pandas DataFrame
2. Format/clean the DataFrame
3. Store the DataFrame into a SQLite database
"""

import re
import pandas as pd
import sqlite3
from pathlib import Path
import glob

try:
    from IPython.display import display
except ImportError:
    def display(x): print(x)


class CSVtoSQLite:

    clean_counter = 0   # internal tracker for dfCleanTest_(num)

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.df = None
        self.current_prefix = None   # prefix based on filename

    def load_csv(self) -> pd.DataFrame:
        """Load a CSV file into a pandas df"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        self.df = pd.read_csv(self.csv_path)

        # Create prefix based on filename
        self.current_prefix = self.csv_path.stem

        return self.df

    def normalize_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize numeric score values to be out of 100"""

        # Find all score columns
        score_cols = [
            col for col in df.columns
            if re.match(r"^Q\d+$", col)
            or re.match(r"^Grades\d+$", col)
            or col == "score"
        ]

        # Normalize each score column to scale of 100
        for col in score_cols:
            max_val = df[col].max()
            df[col] = (df[col] / max_val * 100) if max_val > 0 else 0

        return df

    def clean_dataframe(self) -> pd.DataFrame:
        """
        Clean DataFrame:
        -> Rename columns
        -> Replace nulls with 0
        -> Drop redundant columns
        -> Keep only highest score per student if duplicates exist
        """

        if self.df is None:
            raise ValueError("DataFrame is not loaded. Call load_csv() first.")

        CSVtoSQLite.clean_counter += 1
        test_num = CSVtoSQLite.clean_counter

        # Save original DataFrame
        old_name = f"dftest_{test_num}"
        setattr(self, old_name, self.df.copy())

        df = self.df.copy()

        # Clean column names
        clean_cols = []
        for col in df.columns:
            col_clean = col.strip()
            col_clean = re.sub(r"[ /?]", "", col_clean)  # remove spaces, /, ?
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

        # Save cleaned df's
        clean_name = f"dfCleanTest_{test_num}"
        setattr(self, clean_name, df.copy())

        # Normalize scores to 100
        df = self.normalize_scores(df)

        # Save normalized df's
        formatted_name = f"dfFormattedCleaned_{test_num}"
        setattr(self, formatted_name, df.copy())

        self.df = df
        return df

    def save_to_sqlite(self, db_path: str, csv_pattern: str = None) -> None:
        """
        Save all DataFrame-like attributes into SQLite
        """

        conn = sqlite3.connect(db_path)

       # ----------- Multiple CSV processing--------------
        if csv_pattern:
            csv_files = glob.glob(csv_pattern)
            display(f"Found {len(csv_files)} CSV files.")

            for csv_file in csv_files:
                converter = CSVtoSQLite(csv_file)
                converter.convert(db_path)  # recursively process each CSV

            conn.close()
            return

        # ---------- Single CSV processing ----------
        prefixes = ("dftest_", "dfCleanTest_", "dfFormattedCleaned_")

        for attr_name in dir(self):
            if attr_name.startswith(prefixes):
                df = getattr(self, attr_name)
                if isinstance(df, pd.DataFrame):

                    # Convert object columns to TEXT for SQLite 
                    for col in df.select_dtypes(include="object").columns:
                        df[col] = df[col].astype(str)

                    # Use filename prefix to avoid collisions
                    final_table = f"{self.current_prefix}_{attr_name}"

                    df.to_sql(final_table, conn, if_exists="replace", index=False)
                    display(f"Saved table '{final_table}' ({len(df)} rows)")

        conn.commit()
        conn.close()
        display(f"Database '{db_path}' created successfully.")

    def convert(self, db_path: str, csv_pattern: str = None) -> None:
        """
        Complete process:
        1) Load CSV
        2) Clean/format it
        3) Save all versions to SQLite
        """
        self.load_csv()
        self.clean_dataframe()
        self.save_to_sqlite(db_path, csv_pattern)
