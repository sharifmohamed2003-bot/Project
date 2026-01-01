

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
plt.show()


class TestResultsAnalyzer:
    """
    Analyse student test results stored in a SQLite database
    and visualise them using Matplotlib.
    """

    @staticmethod
    def get_student_scores(db_path: str, student_id: str) -> pd.DataFrame | None:
        """
        Retrieve all test scores/rows of a given researcher/student from all tables.

        Args:
            db_path (str): Path to the SQLite database
            student_id (str): ID to search for (e.g., "156" or "156.0")

        Returns:
            pd.DataFrame | None: Combined DataFrame of results
        """
        # Normalize the user input to an integer when possible
        sid_raw = str(student_id).strip()
        sid_int = None
        try:
            sid_int = int(float(sid_raw))
        except ValueError:
            pass  # keep as text fallback

        conn = sqlite3.connect(db_path)

        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
            conn
        )["name"].tolist()

        student_data = []

        for table in tables:
            try:
                # Read column names first
                cols = pd.read_sql(f"PRAGMA table_info([{table}]);", conn)["name"].tolist()

                # Detect the ID column
                id_cols = [c for c in cols if any(k in c.lower() for k in ["researchid","researcherid","studentid","student","candidateid","candidate","userid","user","id"])]
                if not id_cols:
                    continue
                id_col = id_cols[0]

                # Query only matching rows directly in SQL
                if sid_int is not None:
                    q = f"SELECT *, '{table}' AS source_table FROM [{table}] WHERE CAST([{id_col}] AS INTEGER) = ?"
                    df_student = pd.read_sql_query(q, conn, params=(sid_int,))
                else:
                    # Fallback for non-numeric IDs
                    q = f"SELECT *, '{table}' AS source_table FROM [{table}] WHERE TRIM(CAST([{id_col}] AS TEXT)) = ?"
                    df_student = pd.read_sql_query(q, conn, params=(sid_raw,))

                if not df_student.empty:
                    student_data.append(df_student)

            except Exception:
                continue

        conn.close()

        if not student_data:
            print(f"No data found for researcher ID: {student_id}")
            return None

        return pd.concat(student_data, ignore_index=True)

    @staticmethod
    
    def plot_student_scores(df: pd.DataFrame, student_id: str) -> None:
        """
        Plot a bar chart of student scores per assessment/table.
        Expects df to include 'source_table' and at least one grade/score column.
        """

        if df is None or df.empty:
            print("No data to plot.")
            return

        df = df.copy()

        # 1) Pick a "score" column robustly
        # Prefer an existing 'score' if you already created one
        if "score" not in df.columns:
            # Try common grade column patterns in your data
            score_candidates = [c for c in df.columns if c.lower().startswith("grade") or c.lower() == "grades"]
            if not score_candidates:
                raise ValueError("No score/grade column found to plot (expected columns like 'Grade...' or 'Grades').")
            score_col = score_candidates[0]
            df["score"] = df[score_col]
        else:
            score_col = "score"

        # Convert to numeric
        df["score"] = pd.to_numeric(df["score"], errors="coerce")

        # one score per table
        if "source_table" in df.columns:
            plot_df = (
                df.dropna(subset=["score"])
                .groupby("source_table", as_index=False)["score"]
                .max()  # keep highest score per table
                .rename(columns={"source_table": "Assessment"})
            )
        else:
            plot_df = df.dropna(subset=["score"]).copy()
            plot_df["Assessment"] = "Assessment"

        if plot_df.empty:
            print("No numeric scores available to plot.")
            return

        # 4) Plot
        plt.figure(figsize=(10, 4))
        plt.bar(plot_df["Assessment"].astype(str), plot_df["score"].astype(float))
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Score")
        plt.title(f"Scores for Researcher ID: {student_id}")
        plt.tight_layout()
        plt.show()
