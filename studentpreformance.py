import sqlite3
import re
import pandas as pd
import matplotlib.pyplot as plt


class StudentPerformance:
    """
    Analyse a student's performance per question for a chosen test table.

    - Absolute performance: student's % per question (0-100)
    - Relative performance: student % - class average % (per question)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ---------- DB helpers ----------
    def list_tables(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            tables = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
                conn
            )["name"].tolist()
        return tables

    def load_table(self, table: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(f"SELECT * FROM '{table}'", conn)

    # ---------- detection helpers ----------
    @staticmethod
    def detect_student_id_col(df: pd.DataFrame) -> str:
        candidates = [c for c in df.columns if "student" in c.lower()]
        if not candidates:
            raise ValueError("Could not find a student id column (no column containing 'student').")
        return candidates[0]

    @staticmethod
    def question_columns(df: pd.DataFrame) -> list[str]:
        qcols = [c for c in df.columns if re.fullmatch(r"Q\d+", str(c))]
        qcols.sort(key=lambda x: int(re.findall(r"\d+", x)[0]))
        if not qcols:
            raise ValueError("No question columns found (expected columns like Q1, Q2, ...).")
        return qcols

    @staticmethod
    def ensure_numeric_0_100(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce").fillna(0)

        # If values look like 0-1 proportions, scale
        if s.max() <= 1.0:
            s = s * 100

        # If values exceed 100, normalise down to 100 (safe fallback)
        if s.max() > 100:
            mx = s.max()
            s = (s / mx) * 100 if mx > 0 else 0

        return s

    @staticmethod
    def pick_default_test_table(tables: list[str]) -> str | None:
        preferred = [t for t in tables if "_dfFormattedCleanTest_" in t]
        if preferred:
            return preferred[0]
        return tables[0] if tables else None

    # main API 
    def analyse(self, student_id: str, table: str | None = None) -> pd.DataFrame:
        tables = self.list_tables()
        if not tables:
            raise FileNotFoundError("No tables found in the database.")

        if table is None:
            table = self.pick_default_test_table(tables)

        if table not in tables:
            raise ValueError(f"Table '{table}' not found.")

        df = self.load_table(table)
        sid_col = self.detect_student_id_col(df)

        df_student = df[df[sid_col].astype(str) == str(student_id)].copy()
        if df_student.empty:
            raise ValueError(f"No row found for student_id={student_id} in '{table}'.")

        qcols = self.question_columns(df)

        # Convert question cols to comparable 0-100 numeric
        for c in qcols:
            df[c] = self.ensure_numeric_0_100(df[c])
            df_student[c] = self.ensure_numeric_0_100(df_student[c])

        # If multiple attempts exist, keep best attempt by total across questions
        if len(df_student) > 1:
            df_student["_totalQ"] = df_student[qcols].sum(axis=1)
            df_student = (
                df_student.sort_values("_totalQ", ascending=False)
                          .head(1)
                          .drop(columns="_totalQ")
            )

        student_row = df_student.iloc[0]
        averages = df[qcols].mean(numeric_only=True)
        student_scores = student_row[qcols].astype(float)

        absolute = student_scores
        relative = student_scores - averages

        result = pd.DataFrame({
            "Question": qcols,
            "Absolute (%)": absolute.values,
            "Average (%)": averages.values,
            "Relative (Student - Avg)": relative.values
        })

        return result

    def plot(self, result_df: pd.DataFrame, student_id: str, table: str) -> None:
        qcols = result_df["Question"].tolist()

        # Absolute vs average
        plt.figure(figsize=(12, 6))
        x = range(len(qcols))
        plt.bar([i - 0.2 for i in x], result_df["Absolute (%)"], width=0.4, label="Student (Absolute %)")
        plt.bar([i + 0.2 for i in x], result_df["Average (%)"], width=0.4, label="Class Average (%)")
        plt.xticks(list(x), qcols)
        plt.ylim(0, 105)
        plt.title(f"Absolute Performance per Question | Student {student_id} | {table}")
        plt.xlabel("Question")
        plt.ylabel("Percentage")
        plt.legend()
        plt.tight_layout()
        plt.show()

        # Relative performance
        plt.figure(figsize=(12, 5))
        plt.axhline(0, linewidth=1)
        plt.bar(qcols, result_df["Relative (Student - Avg)"])
        plt.title(f"Relative Performance per Question (Student - Avg) | Student {student_id} | {table}")
        plt.xlabel("Question")
        plt.ylabel("Points vs Average")
        plt.tight_layout()
        plt.show()



