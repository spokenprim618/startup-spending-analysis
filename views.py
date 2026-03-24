import sqlite3

def create_views(db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("""
        CREATE VIEW IF NOT EXISTS ca_startups AS
        SELECT * FROM startups WHERE state = 'California'
        """)

        cursor.execute("""
        CREATE VIEW IF NOT EXISTS ny_startups AS
        SELECT * FROM startups WHERE state = 'New York'
        """)

        cursor.execute("""
        CREATE VIEW IF NOT EXISTS fl_startups AS
        SELECT * FROM startups WHERE state = 'Florida'
        """)
        
        conn.commit()

create_views("./db/startups_raw.db")
create_views("./db/startups_clean_median.db")
create_views("./db/startups_clean_zero.db")

print("Views created successfully")