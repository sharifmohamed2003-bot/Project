"""
CSV to SQLite Converter
#################################################
This module defines a class that can:
1. Load a CSV into a pandas DataFrame
2. Format/clean the DataFrame
3. Store the DataFrame into a SQLite database
"""

import pandas as pd
import sqlite3
import re
from pathlib import Path


class CSVtoSQLite:
    """
    A utility class for converting and processing CSV files into SQLite database tables.
    This class handles the complete pipeline of loading CSV data, standardizing column names,
    normalizing scores, and saving processed dataframes to a SQLite database. It supports
    handling multiple test formats with question columns (Q1, Q2, etc.), grade columns,
    and student identifiers.
    Attributes:
        clean_counter (int): Class-level counter tracking the number of cleaned dataframes.
        csv_path (Path): Path object pointing to the input CSV file.
        df (pd.DataFrame): Currently loaded or processed dataframe.
        current_prefix (str): Stem of the CSV filename, used as prefix for SQLite table names.
        max_map (dict): Dictionary mapping column names to their maximum possible values
                        extracted from column headers (e.g., denominators in "Q1/100").
    Methods:
        load_csv() -> pd.DataFrame:
            Loads CSV file from the specified path and initializes current_prefix.
        _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
            Standardizes column names by removing special characters and extracting
            denominators from headers. Handles Q columns, grade columns, and state/time columns.
        normalize_scores(df: pd.DataFrame) -> pd.DataFrame:
            Normalizes all score columns to a 0-100 scale using the maximum values
            from headers or observed data.
        clean_dataframe() -> pd.DataFrame:
            Performs complete data cleaning pipeline: standardizes columns, handles nulls,
            removes redundant columns, deduplicates by student_id (keeping highest score),
            and normalizes scores. Stores intermediate versions as class attributes.
        save_to_sqlite() -> None:
            Persists all processed dataframes (raw, cleaned, and formatted versions)
            to a SQLite database with table names prefixed by the CSV filename.
        convert() -> None:
            Executes the complete conversion workflow: load_csv() → clean_dataframe() → save_to_sqlite().
    """

    clean_counter = 0

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.df = None
        self.current_prefix = None
        self.max_map = {}   # store true max from header


    # load CSV
    def load_csv(self) -> pd.DataFrame:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"{self.csv_path} not found")

        self.df = pd.read_csv(self.csv_path)
        self.current_prefix = self.csv_path.stem
        return self.df


    # standardise column names + capture denominators from headers
    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self.max_map = {}
        new_cols = []
        seen = {}

        for col in df.columns:
            raw = str(col).strip()
            compact = re.sub(r"[^A-Za-z0-9]", "", raw).lower()

            denom = None

            # redundant cols -> consistent names so drop works
            if compact == "state":
                name = "state"
            elif compact in {"timetaken", "timetakenminutes", "timetakenmins", "timetakenmin", "timetaken"}:
                name = "timetaken"

            # Q columns like "Q 1 /500" -> Q1 and store 500
            else:
                m = re.match(r"^Q\s*(\d+)(?:\s*/\s*(\d+))?.*$", raw, flags=re.IGNORECASE)
                if m:
                    name = f"Q{int(m.group(1))}"
                    denom = int(m.group(2)) if m.group(2) else None
                else:
                    # Grade columns like "Grade/10000" or "Grades/600" -> score and store max
                    m = re.match(r"^Grades?\s*/\s*(\d+).*$", raw, flags=re.IGNORECASE)
                    if m:
                        name = "score"
                        denom = int(m.group(1))
                    else:
                        # fallback: your original cleaning
                        name = re.sub(r"[ /?\-]", "", raw.strip())

            # avoid collisions
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 1

            new_cols.append(name)
            if denom is not None:
                self.max_map[name] = denom

        out = df.copy()
        out.columns = new_cols
        return out


    # normalize scores to 100
    def normalize_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        score_cols = [
            col for col in df.columns
            if re.match(r"^Q\d+$", col)
            or re.match(r"^Grades\d+$", col)
            or col == "score"
        ]

        for col in score_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # use true max from header if available; otherwise fallback to observed max
            max_possible = self.max_map.get(col, None)
            if max_possible is None:
                max_possible = df[col].max()

            if max_possible > 0:
                df[col] = (df[col] / max_possible) * 100
            else:
                df[col] = 0

        return df


    # clean DF's
    def clean_dataframe(self) -> pd.DataFrame:

        if self.df is None:
            raise ValueError("Call load_csv() first")

        CSVtoSQLite.clean_counter += 1
        test_num = CSVtoSQLite.clean_counter

        # keep original DF
        setattr(self, f"dftest_{test_num}", self.df.copy())

        df = self.df.copy()

        #rename columns using the standardiser (handles Q1, score, state, timetaken)
        df = self.standardize_columns(df)

        # replace nulls with 0
        df = df.fillna(0)

        # drop rows that are completely empty
        df = df.dropna(how="all")

        # drop redundant columns
        for col in ["state", "timetaken"]:
            if col in df.columns:
                df = df.drop(columns=col)

        # keep highest score per student (uses existing "student_id" if present)
        if "student_id" in df.columns:
            score_cols = [
                c for c in df.columns
                if c.startswith("Q") or c.startswith("Grades") or c == "score"
            ]

            if score_cols:
                df["total_score"] = df[score_cols].sum(axis=1)
                df = (
                    df.sort_values("total_score", ascending=False)
                      .drop_duplicates("student_id")
                      .drop(columns="total_score")
                )

        # save cleaned df's
        setattr(self, f"dfCleanTest_{test_num}", df.copy())

        # normalise scores to scale of 100
        df = self.normalize_scores(df)

        # rename formatted df's
        setattr(self, f"dfFormattedCleanTest_{test_num}", df.copy())

        self.df = df
        return df


    # Save to SQLite
    def save_to_sqlite(self) -> None:

        conn = sqlite3.connect("CWDatabase.db")

        prefixes = ("dftest_", "dfCleanTest_", "dfFormattedCleanTest_")

        for attr in dir(self):
            if attr.startswith(prefixes):
                df = getattr(self, attr)
                if isinstance(df, pd.DataFrame):
                    df.to_sql(
                        f"{self.current_prefix}_{attr}",
                        conn,
                        if_exists="replace",
                        index=False
                    )

        conn.commit()
        conn.close()


    def convert(self) -> None:
        self.load_csv()
        self.clean_dataframe()
        self.save_to_sqlite()
