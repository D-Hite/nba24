# %%
import pandas as pd
import glob
import time
import duckdb



# %%
conn = duckdb.connect('firstdb.db')
# %%
raw_line_filepath = 'DATA/raw/lines/'
lines_csv_files = glob.glob(f'{raw_line_filepath}*.csv')
if not lines_csv_files:
    print(f'create_line_table error: no file in {raw_line_filepath}')


try:
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_line_table as
        SELECT *
        FROM read_csv_auto('{lines_csv_files[0]}')
    """).df()

    for file in lines_csv_files[1:]:
        conn.execute(f"""
            INSERT INTO raw_line_table
            SELECT * FROM read_csv_auto('{file}')
            """)
except Exception as e:
    print(f"error: {e}")

print('SUCCESSFULLY MADE LINE TABLE')
# %%
conn.execute("SELECT * from raw_line_table order by random() limit 100").df()


# %%
conn.execute("""SELECT CAST(
            SUBSTRING(CAST(date AS VARCHAR), 1, 4) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 5, 2) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 7, 2)
            as DATE
            )
            AS DATE
             
             
            from raw_line_table order by random() limit 100""").df()
# %%


conn.execute("""SHOW TABLES""").df()
# %%
conn.execute("""select *
             from log_table limit 100""").df()

# %%
conn.execute("""
             
            WITH RAW_DATA AS (
             
            SELECT CAST(
            SUBSTRING(CAST(date AS VARCHAR), 1, 4) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 5, 2) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 7, 2)
            as DATE
            )
            AS P_DATE,
            
            team,
            line,
            total,
             

            
            from raw_line_table
             
             
             )
             
            select
             lt.GAME_ID::int,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             inner join log_table lt
             on lt.GAME_DATE::DATE == rd.P_DATE
             and lt.TEAM_NAME ilike '%'|| rd.team || '%'
             
             
             
             
             
             """).df()
# %%
