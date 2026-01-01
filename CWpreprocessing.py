"""
CSV to SQLite Converter
#################################################
This module defines a class that can:
1. Load a CSV into a pandas DataFrame
2. Format/clean the DataFrame
3. Store the DataFrame into a SQLite database

This was done by Mohamed Amin Asharif (F515137)
started: 27/11/2025
#################################################
"""

import pandas as pd
import sqlite3
import re
from pathlib import Path


class CSVtoSQLite:
  

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


    # standardise column names + capture denoms from headers
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
                        # if everything else fails
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

            # uses true max from header if available
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

        # treat common “missing” placeholders as NA
        df = df.replace(
            to_replace=[r"^\s*$", "-", "–", "—", "N/A", "NA", "na"],
            value=pd.NA,
            regex=True
        )

        #  dropy empty rows
        df = df.dropna(how="all")

        # fill remaining missing values with 0
        df = df.fillna(0)

        # drop redundant columns
        for col in ["state", "timetaken"]:
            if col in df.columns:
                df = df.drop(columns=col)

        # keep highest score per student
        if "research_id" in df.columns:
            score_cols = [
                c for c in df.columns
                if c.startswith("Q") or c.startswith("Grades") or c == "score"
            ]

            if score_cols:
                df["total_score"] = df[score_cols].sum(axis=1)
                df = (
                    df.sort_values("total_score", ascending=False)
                      .drop_duplicates("research_id")
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
