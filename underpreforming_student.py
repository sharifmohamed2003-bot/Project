# underperformingStudent.py
import sqlite3
import re
import pandas as pd
import matplotlib.pyplot as plt


class UnderperformingStudents:
    """
    Identify underperforming students across ALL tests, sorted by summative grade.

    Requirements:
    - Students sorted by summative online test grade (ascending)
    - Highlight lowest grade among formative tests for each student
    - Matplotlib visualisation
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    # DB helpers 
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

    #  detection helpers 

    def detect_student_id_col(self, df):
        """
        Detect identifier column (student / researcher / candidate).
        """
        cols = list(df.columns)
        norm = [c.lower().replace(" ", "").replace("_", "").replace("-", "") for c in cols]

        # The ID used in the CSV is researcher id, however to future proof this ive added other possibilities
        id_keywords = [
            "studentid", "student",
            "researcherid", "researcher",
            "candidateid", "candidate",
            "learnerid", "learner",
            "userid", "user",
            "id"
        ]

        for key in id_keywords:
            for original, normalized in zip(cols, norm):
                if key in normalized:
                    return original

        raise ValueError(
            "Could not find a student/researcher ID column. "
            "Expected a column like 'Student ID', 'Researcher ID', or similar."
        )


    @staticmethod
    def ensure_numeric_0_100(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce").fillna(0)
        if s.max() <= 1.0:
            s = s * 100
        if s.max() > 100:
            mx = s.max()
            s = (s / mx) * 100 if mx > 0 else 0
        return s

    @staticmethod
    def formatted_test_tables(tables: list[str]) -> list[str]:
        return [t for t in tables if "_dfFormattedCleanTest_" in t]

    def detect_summative_table(self, tables: list[str]) -> str | None:
        lowered = [(t, t.lower()) for t in tables]
        for key in ["summative", "final", "online", "exam"]:
            matches = [t for t, tl in lowered if key in tl]
            if matches:
                return matches[0]

        #choose formatted table with highest trailing number
        candidates = self.formatted_test_tables(tables)
        if not candidates:
            return tables[0] if tables else None

        def trailing_num(name: str) -> int:
            m = re.search(r"_dfFormattedCleanTest_(\d+)$", name)
            return int(m.group(1)) if m else -1

        candidates.sort(key=trailing_num, reverse=True)
        return candidates[0]

    def get_total_score(self, df: pd.DataFrame) -> pd.Series:
        cols_lower = {c.lower(): c for c in df.columns}
        if "score" in cols_lower:
            return self.ensure_numeric_0_100(df[cols_lower["score"]])

        qcols = [c for c in df.columns if re.fullmatch(r"Q\d+", str(c))]
        if qcols:
            tmp = df[qcols].copy()
            for c in qcols:
                tmp[c] = self.ensure_numeric_0_100(tmp[c])
            return self.ensure_numeric_0_100(tmp.sum(axis=1))

        #sum numeric columns excluding student id columns
        sid_guess = [c for c in df.columns if "student" in c.lower()]
        numeric = df.drop(columns=sid_guess, errors="ignore").select_dtypes(include="number")
        if numeric.shape[1] == 0:
            return pd.Series([0] * len(df), index=df.index)
        return self.ensure_numeric_0_100(numeric.sum(axis=1))

    # main API
    def build_report(
        self,
        summative_table: str | None = None,
        threshold: float = 40.0
    ) -> tuple[pd.DataFrame, str]:
        tables = self.list_tables()
        tests = self.formatted_test_tables(tables) or tables
        if not tests:
            raise FileNotFoundError("No tables found in the database.")

        if summative_table is None:
            summative_table = self.detect_summative_table(tests)

        if summative_table not in tables:
            raise ValueError(f"Summative table '{summative_table}' not found.")

        # Summative best per student
        df_sum = self.load_table(summative_table)
        sid_col = self.detect_student_id_col(df_sum)

        df_sum["_total"] = self.get_total_score(df_sum)
        df_sum_best = (
            df_sum.sort_values("_total", ascending=False)
                  .drop_duplicates(subset=[sid_col])
                  .copy()
        )
        df_sum_best["_total"] = self.ensure_numeric_0_100(df_sum_best["_total"])

        #compute each student's lowest formative score
        formative_tables = [t for t in tests if t != summative_table]
        formative_min: dict[str, tuple[float, str]] = {}

        for t in formative_tables:
            df_f = self.load_table(t)
            sid_f = self.detect_student_id_col(df_f)
            df_f["_total"] = self.ensure_numeric_0_100(self.get_total_score(df_f))

            df_f_best = (
                df_f.sort_values("_total", ascending=False)
                    .drop_duplicates(subset=[sid_f])
            )

            for _, row in df_f_best.iterrows():
                sid = str(row[sid_f])
                sc = float(row["_total"])
                if sid not in formative_min or sc < formative_min[sid][0]:
                    formative_min[sid] = (sc, t)

        # Combine into report
        rows = []
        for _, r in df_sum_best.iterrows():
            sid = str(r[sid_col])
            sum_score = float(r["_total"])
            fmin_score, fmin_table = formative_min.get(sid, (float("nan"), "N/A"))

            rows.append({
                "student_id": sid,
                "summative_table": summative_table,
                "summative_score": sum_score,
                "lowest_formative_score": fmin_score,
                "lowest_formative_table": fmin_table,
                "is_underperforming": sum_score < threshold
            })

        report = pd.DataFrame(rows).sort_values("summative_score", ascending=True).reset_index(drop=True)
        return report, summative_table

    def plot_underperformers(self, report: pd.DataFrame, summative_table: str, threshold: float) -> None:
        under = report[report["is_underperforming"]].copy()
        if under.empty:
            print("No underperforming students found for this threshold.")
            return

        plt.figure(figsize=(12, 6))
        plt.bar(under["student_id"], under["summative_score"])
        plt.title(f"Underperforming Students (Summative: {summative_table}, Threshold < {threshold})")
        plt.xlabel("Student ID")
        plt.ylabel("Summative Score (%)")
        plt.xticks(rotation=60, ha="right")
        plt.tight_layout()
        plt.show()

        plt.figure(figsize=(12, 6))
        plt.bar(under["student_id"], under["summative_score"], label="Summative (%)")
        plt.scatter(under["student_id"], under["lowest_formative_score"], label="Lowest Formative (%)")
        plt.title("Summative vs Lowest Formative (Underperforming Students)")
        plt.xlabel("Student ID")
        plt.ylabel("Score (%)")
        plt.xticks(rotation=60, ha="right")
        plt.legend()
        plt.tight_layout()
        plt.show()


