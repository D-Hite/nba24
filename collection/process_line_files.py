# %%
import pandas as pd
import glob
import time
import duckdb



# %%
conn = duckdb.connect('firstdb.db')


# %% 
## remake line table from processed files
line_data_path = 'DATA/lines/'
lines_csv_files = glob.glob(f'{line_data_path}*.csv')
if not lines_csv_files:
    print(f'create_line_table error: no file in {line_data_path}')


try:
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
except Exception as e:
    print(f"create_line_table error: {e}")

print('SUCCESSFULLY MADE LINE TABLE')



# %%
### make intermediate raw line data table
raw_line_filepath = 'DATA/raw/lines/'
lines_csv_files = glob.glob(f'{raw_line_filepath}*.csv')
if not lines_csv_files:
    print(f'create_line_table error: no file in {raw_line_filepath}')


try:
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_line_table as
        SELECT
        date::VARCHAR as date
        ,team::VARCHAR as team
        ,site::VARCHAR as ha
        ,'o:team'::VARCHAR as other_team
        ,TRY_CAST(line AS DOUBLE) as line
        ,TRY_CAST(total AS DOUBLE) as total

        FROM read_csv_auto('{lines_csv_files[0]}')
    """).df()

    for file in lines_csv_files[1:]:
        conn.execute(f"""
            INSERT INTO raw_line_table
            SELECT
            date::VARCHAR as date
            ,team::VARCHAR as team
            ,site::VARCHAR as ha
            ,'o:team'::VARCHAR as other_team
            ,TRY_CAST(line AS DOUBLE) as line
            ,TRY_CAST(total AS DOUBLE) as total
                     
            FROM read_csv_auto('{file}')
            """)
except Exception as e:
    print(f"error: {e}")

print('SUCCESSFULLY MADE RAW LINE TABLE')


# %%
### MAKE MAPPING TABLE
conn.execute("""CREATE OR REPLACE TABLE LINE_TEAM_MAPPING_TABLE
             (raw_data_team_name VARCHAR,
             log_table_team_name VARCHAR)""").df()

conn.execute("""INSERT INTO LINE_TEAM_MAPPING_TABLE (raw_data_team_name,log_table_team_name)
             VALUES
('Jazz','Utah Jazz'),
('Wizards','Washington Wizards'),
('Spurs','San Antonio Spurs'),
('Hornets','Charlotte Hornets'),
('Bucks','Milwaukee Bucks'),
('Grizzlies','Memphis Grizzlies'),
('Hawks','Atlanta Hawks'),
('Pacers','Indiana Pacers'),
('Pistons','Detroit Pistons'),
('Timberwolves','Minnesota Timberwolves'),
('Seventysixers','Philadelphia 76ers'),
('Nuggets','Denver Nuggets'),
('Thunder','Oklahoma City Thunder'),
('Celtics','Boston Celtics'),
('Raptors','Toronto Raptors'),
('Cavaliers','Cleveland Cavaliers'),
('Warriors','Golden State Warriors'),
('Knicks','New York Knicks'),
('Mavericks','Dallas Mavericks'),
('Lakers','Los Angeles Lakers'),
('Nets','Brooklyn Nets'),
('Magic','Orlando Magic'),
('Heat','Miami Heat'),
('Trailblazers','Portland Trail Blazers'),
('Suns','Phoenix Suns'),
('Clippers','LA Clippers'),
('Rockets','Houston Rockets'),
('Bulls','Chicago Bulls'),
('Kings','Sacramento Kings'),
('Pelicans','New Orleans Pelicans')""").df()




# %%
conn.execute("SELECT * from raw_line_table order by random() limit 100").df()



# %%
## DEV QUERY
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
             lt.GAME_ID::int as GAME_ID,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             join LINE_TEAM_MAPPING_TABLE mp
                on mp.raw_data_team_name = rd.team
             inner join log_table lt
             on lt.GAME_DATE::DATE == rd.P_DATE
             and lt.TEAM_NAME = mp.log_table_team_name  

             WHERE GAME_ID =  22201104
             
             
             """).df()

# %%
### INSERTING INTO LINE TABLE
conn.execute("""
             INSERT into LINES_TABLE
             (
             
             
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
             lt.GAME_ID::int as GAME_ID,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             join LINE_TEAM_MAPPING_TABLE mp
                on mp.raw_data_team_name = rd.team
             inner join log_table lt
             on lt.GAME_DATE::DATE == rd.P_DATE
             and lt.TEAM_NAME = mp.log_table_team_name   
             
             )
             
             """).df()

# %%
conn.execute("""select count(*) from lines_table
             WHERE GAME_DATE::DATE BETWEEN '2023-11-03'::DATE AND '2024-06-17'::DATE""").df()
# %%
conn.execute("""select * from lines_table
             order by GAME_DATE DESC limit 100""").df()

# %%
conn.execute("""select * from lines_table
             where GAME_ID = 22201104""").df()




















## DEV
# %%

# %%
conn.execute("""select distinct TEAM_NAME from LOG_TABLE
             order by team_name""").df()
# %%
# %%
conn.execute("""select distinct team from raw_line_table
             limit 100""").df()
# %%
# %%
conn.execute("""CREATE OR REPLACE TABLE LINE_TEAM_MAPPING_TABLE
             (raw_data_team_name VARCHAR,
             log_table_team_name VARCHAR)""").df()

# %%
conn.execute("""INSERT INTO LINE_TEAM_MAPPING_TABLE (raw_data_team_name,log_table_team_name)
             VALUES
('Jazz','Utah Jazz'),
('Wizards','Washington Wizards'),
('Spurs','San Antonio Spurs'),
('Hornets','Charlotte Hornets'),
('Bucks','Milwaukee Bucks'),
('Grizzlies','Memphis Grizzlies'),
('Hawks','Atlanta Hawks'),
('Pacers','Indiana Pacers'),
('Pistons','Detroit Pistons'),
('Timberwolves','Minnesota Timberwolves'),
('Seventysixers','Philadelphia 76ers'),
('Nuggets','Denver Nuggets'),
('Thunder','Oklahoma City Thunder'),
('Celtics','Boston Celtics'),
('Raptors','Toronto Raptors'),
('Cavaliers','Cleveland Cavaliers'),
('Warriors','Golden State Warriors'),
('Knicks','New York Knicks'),
('Mavericks','Dallas Mavericks'),
('Lakers','Los Angeles Lakers'),
('Nets','Brooklyn Nets'),
('Magic','Orlando Magic'),
('Heat','Miami Heat'),
('Trailblazers','Portland Trail Blazers'),
('Suns','Phoenix Suns'),
('Clippers','LA Clippers'),
('Rockets','Houston Rockets'),
('Bulls','Chicago Bulls'),
('Kings','Sacramento Kings'),
('Pelicans','New Orleans Pelicans')""").df()

# %%

('Jazz','Utah Jazz'),
('Wizards','Washington Wizards'),
('Spurs','San Antonio Spurs'),
('Hornets','Charlotte Hornets'),
('Bucks','Milwaukee Bucks'),
('Grizzlies','Memphis Grizzlies'),
('Hawks','Atlanta Hawks'),
('Pacers','Indiana Pacers'),
('Pistons','Detroit Pistons'),
('Timberwolves','Minnesota Timberwolves'),
('Seventysixers','Philadelphia 76ers'),
('Nuggets','Denver Nuggets'),
('Thunder','Oklahoma City Thunder'),
('Celtics','Boston Celtics'),
('Raptors','Toronto Raptors'),
('Cavaliers','Cleveland Cavaliers'),
('Warriors','Golden State Warriors'),
('Knicks','New York Knicks'),
('Mavericks','Dallas Mavericks'),
('Lakers','Los Angeles Lakers'),
('Nets','Brooklyn Nets'),
('Magic','Orlando Magic'),
('Heat','Miami Heat'),
('Trailblazers','Portland Trail Blazers'),
('Suns','Phoenix Suns'),
('Clippers','LA Clippers'),
('Rockets','Houston Rockets'),
('Bulls','Chicago Bulls'),
('Kings','Sacramento Kings'),
('Pelicans','New Orleans Pelicans')