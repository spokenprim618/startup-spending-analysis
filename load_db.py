import pandas as pd
import sqlite3
import os
import sqlalchemy as sa
import os
print("Working directory:", os.getcwd())
print("Absolute DB path:", os.path.abspath("./db/startups_raw.db"))

df_raw=pd.read_csv('./csv/50_Startups.csv')
df_raw.columns = [
    "rd_spend",
    "administration",
    "marketing_spend",
    "state",
    "profit"
]

df_clean_median = df_raw.copy()

numeric_cols = df_clean_median.select_dtypes(include='number').columns
df_clean_median[numeric_cols] = df_clean_median[numeric_cols].fillna(
    df_clean_median[numeric_cols].median()
)

df_clean_median["state"] = df_clean_median["state"].fillna("Unknown")

df_clean_zero = df_raw.copy()

numeric_cols = df_clean_zero.select_dtypes(include='number').columns
df_clean_zero[numeric_cols] = df_clean_zero[numeric_cols].fillna(0)

df_clean_zero["state"] = df_clean_zero["state"].fillna("Unknown")

def create_db(df, db_path):
    print(f"Creating database at {db_path}")

    if os.path.exists(db_path):
        answer = input(f"{db_path} exists. Recreate? (y/n) ")
        if answer.lower() != 'y':
            return
        os.remove(db_path)

    with sqlite3.connect(db_path, isolation_level='IMMEDIATE') as conn:
        conn.execute("PRAGMA foreign_keys = 1")

        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS startups (
            id INTEGER PRIMARY KEY,
            rd_spend REAL,
            administration REAL,
            marketing_spend REAL,
            state TEXT,
            profit REAL
        )
        """)

    engine = sa.create_engine(f"sqlite:///{db_path}")

    df.to_sql("startups", engine, if_exists="append", index=False)

    # ---------- VERIFICATION ----------
    if os.path.exists(db_path):
        print(f"Database file created: {db_path}")
    else:
        print("ERROR: Database file was not created")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check tables
        cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        """)
        tables = cursor.fetchall()
        print("Tables in database:", tables)

        # Count rows
        cursor.execute("SELECT COUNT(*) FROM startups")
        count = cursor.fetchone()[0]
        print(f"Rows inserted: {count}")

create_db(df_raw, "./db/startups_raw.db")

create_db(df_clean_median, "./db/startups_clean_median.db")

create_db(df_clean_zero, "./db/startups_clean_zero.db")

print("Database created successfully")