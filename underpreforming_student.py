# underperformingStudent.py
import sqlite3
import re
import pandas as pd
import matplotlib.pyplot as plt


class UnderperformingStudents:
    """
   The underperforming students analyser can:
    - sort students by summative test score
    - highlight the lowest grade among formative tests for each student
    - use matplotlib for graph and table visualisation
    This was done by Mohamed Amin Asharif (F515137)
   started: 23/12/2025
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    ############# DB helpers ###################
    def list_tables(self) -> list[str]:
        """"List all tables in the SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)

        return tables["name"].tolist()

    def load_table(self, table: str) -> pd.DataFrame:
        """Load the DB table into pandas"""
        with sqlite3.connect(self.db_path) as conn:

            return pd.read_sql(f"SELECT * FROM '{table}'", conn) #loads the specified table from the database into a pandas DataFrame

    ##################### detection helpers ##################
    """
    this is the detector for the student id column as the studentpreformance file but adapted for underperforming students
    """
    def detect_id_col(self, df):
        """
        Detect identifier column (student / researcher / candidate).
        """
        cols = list(df.columns)
        

        # The ID used in the CSV is researcher id, however to future proof this ive added other possibilities
        id_keywords = ["studentid", "student","researchid", "research","candidateid", "candidate","id"]

        for col in df.columns:
            cleaned = re.sub(r"[^a-z0-9]", "", str(col).lower())
            for key in id_keywords: ## insertion sort method of checking if any of the keywords are in the cleaned column name
                if key in cleaned:
                    return col

        raise ValueError(f"No ID found. Columns were: {cols}")


    @staticmethod
    def formatted_test_tables(tables: list[str]) -> list[str]:

        return [t for t in tables if "_dfFormattedCleanTest_" in t]## will filter out the other saved df's to only show the formatted + cleaned versions

    def detect_summative_table(self, tables: list[str]) -> str | None:

        #choose formatted table with highest number
        candidates = self.formatted_test_tables(tables)## gets all the formatted test tables
        if not candidates:
            return tables[0] if tables else None## if no formatted tables exist return the first table and if none exist return nothing

        def end_num(name: str) -> int:

            m = re.search(r"_dfFormattedCleanTest_(\d+)$", name) ## uses the number at the end of the name and uses the most recent or biggest
            return int(m.group(1)) if m else -1

        candidates.sort(key=end_num, reverse=True)
        return candidates[0]##picks the most recent formatted table

    def get_total_score(self, df: pd.DataFrame) -> pd.Series:
    # Prefer explicit score column
        if "score" in df.columns:
            s = pd.to_numeric(df["score"], errors="coerce").fillna(0)
            return s

        # Otherwise sum Q columns
        qcols = [c for c in df.columns if re.fullmatch(r"Q\d+", str(c))]
        if qcols:
            qdf = df[qcols].apply(pd.to_numeric, errors="coerce").fillna(0)
            return qdf.sum(axis=1)

    ######################### main API #################################### 
    def build_report(self,summative_table: str | None = None,threshold: float = 40.0) -> tuple[pd.DataFrame, str]:
        
        tables = self.list_tables()#gets all the tables 
        tests = self.formatted_test_tables(tables)#gets the formatted ones only
        
        if summative_table is None:
            summative_table = self.detect_summative_table(tests)###auto detect feture if no summative table is given

        # Summative best per student
        df_sum = self.load_table(summative_table)
        id_col = self.detect_id_col(df_sum)

        df_sum["total"] = self.get_total_score(df_sum)

        df_sum_best = (
            df_sum.sort_values("total", ascending=False)
                .drop_duplicates(subset=[id_col], keep="first")
        )
        #compute each student's lowest formative score
        formative_tables = [t for t in tests if t != summative_table] ##removes the summative table from the formative tables list
        formative_min: dict[str, tuple[float, str]] = {}## this stores the score as a str and the score as a float and the table name as a str

        for t in formative_tables:
            df_for = self.load_table(t)#load table in df
            id_for = self.detect_id_col(df_for)#detect id column

            df_for["total"] = self.get_total_score(df_for)#get total score for each student using the function

            #keep highest score per student
            df_for_best = (
                df_for.sort_values("total", ascending=False)
                    .drop_duplicates(subset=[id_for], keep="first")
            )

            for _, row in df_for_best.iterrows():
                id = str(row[id_for])# convert id to string
                stud_score = float(pd.to_numeric(row["total"], errors="coerce") or 0)# get student score as int

                if id not in formative_min or stud_score < formative_min[id][0]:#if id isnt in the dict store it OR if currentscore is lower replace
                    formative_min[id] = (stud_score, t)

        """now we are looping through each students best sum score and lowest best form score to build the report"""
        rows = [] # becomes the df
        for _, r in df_sum_best.iterrows():# loop through each students best summative score
            id = str(r[id_col])#this is the ID
            sum_score = float(r["total"])# total sum score
            formin_score, formin_table = formative_min.get(id, (None, "N/A"))# get lowest form score and store N/A if none exist

            rows.append({"student_id": id, "summative_table": summative_table, "summative_score": sum_score,
                         "lowest_formative_score": formin_score,"lowest_formative_table": formin_table,"is_underperforming": sum_score < threshold})
            
        ##makes the dicts a df, sorts from lowest sum to highest and makes index 0 to n-1
        report = pd.DataFrame(rows).sort_values("summative_score", ascending=True).reset_index(drop=True)
        return report, summative_table
    

#################### BUILDING THE REPORT #######################
    def plot_underperformers(self, report: pd.DataFrame, summative_table: str, threshold: int) -> None:

        under = report[report["is_underperforming"]].copy()#filter only underperforming students
        
        if under.empty:
            print("No underperforming students found for this threshold.")
            return

        """ 
        plot 2 bar charts:
         - bar chart 1 - summative of underperforming students, x axis student id, y axis summative score
         - bar chart 2 - summative vs lowest formative, bar for summative, scatter for formative
        """

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
