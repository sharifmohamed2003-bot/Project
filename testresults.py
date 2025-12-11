# Ensure plots display inline
%matplotlib inline

import sqlite3
import pandas as pd
import matplotlib as plt
from IPython.display import display

def get_student_scores(db_path: str, student_id: str):
    """
    retrieve all test scores of a given student from all tables in the database
    
    args:
        db_path (str): Path to the SQLite db
        student_id (str): The ID of the student
    
    returns:
        pd.DataFrame: Combined df of all assessments and scores
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]

    # Collect results from each table
    student_data = []

    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table} WHERE student_id = ?", conn, params=(student_id,))
            if not df.empty:
                df['source_table'] = table  # track which CSV/table it came from
                student_data.append(df)
        except Exception:
            # Ignore tables without student_id column
            continue

    conn.close()

    if not student_data:
        print(f"No data found for student ID: {student_id}")
        return None

    # Combine all tables into one DataFrame
    combined_df = pd.concat(student_data, ignore_index=True)
    return combined_df

def plot_student_scores(df, student_id):
    """
    Plot the student's scores as a bar chart.
    
    Args:
        df (pd.DataFrame): DataFrame of student scores.
        student_id (str): ID of the student.
    """
    # Select numeric columns
    score_cols = df.select_dtypes(include='number').columns.tolist()
    
    # Exclude index-like columns if present
    non_score_cols = ['score'] if 'score' in score_cols else []
    score_cols = [c for c in score_cols if c not in non_score_cols]

    # If no numeric columns, fallback to using 'score' column
    if not score_cols and 'score' in df.columns:
        score_cols = ['score']

    # Melt dataframe for plotting
    plot_df = df.melt(id_vars=['source_table'], value_vars=score_cols, 
                      var_name='Assessment', value_name='Score')

    plt.figure(figsize=(12,6))
    plt.title(f"All Assessments of Student ID: {student_id}")
    plt.bar(plot_df['Assessment'] + " (" + plot_df['source_table'] + ")", plot_df['Score'])
    plt.xticks(rotation=45, ha='right')
    plt.ylabel("Score /100")
    plt.tight_layout()
    plt.show()

