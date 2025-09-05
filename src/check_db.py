import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("../scraper.db")
TABLE_NAME = "offers"

def load_from_db(db_path: Path, table_name: str) -> pd.DataFrame:
    """Load data from the SQLite database into a DataFrame"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"Database not found in {DB_PATH}")
    else:
        df = load_from_db(DB_PATH, TABLE_NAME)
        print(f"Total registers: {len(df)}\n")
        print(df.tail(10))  
