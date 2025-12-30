# Ensure plots display inline 
try:
    get_ipython().run_line_magic('matplotlib', 'inline')
except NameError:
    pass

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


class TestResultsAnalyzer:
    """
    Analyse student test results stored in a SQLite database
    and visualise them using Matplotlib.
    """

    @staticmethod
    def get_student_scores(db_path: str, student_id: str) -> pd.DataFrame | None:
        """
        Retrieve all test scores of a given student from all tables
        in the database.

        *Args:
            db_path (str): Path to the SQLite database
            student_id (str): Student ID to search for

        Returns:
            pd.DataFrame or None: Combined DataFrame of results
        """
        conn = sqlite3.connect(db_path)

        # Get all table names
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'",
            conn
        )["name"].tolist()

        student_data = []

        for table in tables:
            try:
                df = pd.read_sql(f"SELECT * FROM '{table}'", conn)

                # Detect student ID column dynamically
                id_cols = [c for c in df.columns if "student" in c.lower()]
                if not id_cols:
                    continue

                id_col = id_cols[0]

                # Filter rows for the given student
                df_student = df[df[id_col].astype(str) == str(student_id)]

                if not df_student.empty:
                    df_student = df_student.copy()
                    df_student["source_table"] = table
                    student_data.append(df_student)

            except Exception:
                continue

        conn.close()

        if not student_data:
            print(f"No data found for student ID: {student_id}")
            return None

        return pd.concat(student_data, ignore_index=True)

    @staticmethod
    def plot_student_scores(df: pd.DataFrame, student_id: str) -> None:
        """
        Plot the student's assessment scores as a bar chart.

        Args:
            df (pd.DataFrame): DataFrame of student scores
            student_id (str): Student ID
        """
        if "score" not in df.columns:
            raise ValueError("No total 'score' column found for plotting")

        plot_df = df[["source_table", "score"]].copy()

        # Clean table names for display
        plot_df["Assessment"] = plot_df["source_table"].str.split("_df").str[0]

        plt.figure(figsize=(10, 6))
        plt.bar(plot_df["Assessment"], plot_df["score"])
        plt.title(f"All Assessments for Student ID: {student_id}")
        plt.xlabel("Assessment")
        plt.ylabel("Score (Normalised to 100)")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()
