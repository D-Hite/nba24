import pandas as pd
import glob
import time
import duckdb

conn = duckdb.connect('firstdb.db')


current_tables = ['advanced', 'fourfactors', 'misc', 'scoring']


## log data
log_csv_files = glob.glob(f'DATA/raw/log/*.csv')

conn.execute(f"""
    CREATE OR REPLACE TABLE log_table as
    SELECT *
    FROM read_csv_auto('{log_csv_files[0]}')
""").df()

for file in log_csv_files[1:]:
    conn.execute(f"""
        INSERT INTO log_table
        SELECT * FROM read_csv_auto('{file}')
        """)

## line data
lines_csv_files = glob.glob(f'DATA/raw/lines/*.csv')

conn.execute(f"""
    CREATE OR REPLACE TABLE lines_table as
    SELECT *
    FROM read_csv_auto('{lines_csv_files[0]}')
""").df()

for file in lines_csv_files[1:]:
    conn.execute(f"""
        INSERT INTO lines_table
        SELECT * FROM read_csv_auto('{file}')
        """)

## game data
for tp in ['teams', 'players']:
    for kind in current_tables:
        csv_files = glob.glob(f'DATA/raw/teams/{kind}/*.csv')

        conn.execute(f"""
            CREATE OR REPLACE TABLE {tp}_{kind} as
            SELECT *
            FROM read_csv_auto('{csv_files[0]}')
        """).df()


        for file in csv_files[1:]:
            conn.execute(f"""
                INSERT INTO {tp}_{kind}
                SELECT * FROM read_csv_auto('{file}')
                """)
            
### todo: combine tables, processed data, dynamic query for model