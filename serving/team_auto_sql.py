# %%
import pandas as pd
import glob
import time
import duckdb
from os import path



# %%
basepath = path.dirname(__file__)
filepath = path.abspath(path.join(basepath, "..", "collection", "firstdb.db"))

print(filepath)


# %%
conn = duckdb.connect(filepath)

# %%
conn.execute("""SELECT table_schema, table_name
FROM information_schema.tables""").df()

# %%
"""
want to have a dynamic sql quiery that can grab data given the parameters:
columns
team or player data
seasons
offseason or normal
average data, rolling average data or static data
next game outcome data
line data
"""



# %%
rolling_avg_number = 10
columns_wanted = ['EFG_PCT',
    'FTA_RATE',
    'TM_TOV_PCT',
    'OREB_PCT',
    'OPP_EFG_PCT',
    'OPP_FTA_RATE',
    'OPP_TOV_PCT',
    'OPP_OREB_PCT']


# %%

def create_base_team_dataset(cols, roll_number):
    start = f"""CREATE OR REPLACE TABLE processed.TEMP_TEAM_{roll_number}_AVG_DATA AS
        SELECT
        TEAM_ABBREVIATION
        ,SEASON_ID
        ,GAME_ID
        ,GAME_DATE
        ,MATCHUP
        ,PLUS_MINUS
        ,PTS"""
    for i in cols:
        start+=f"\n\t,{i}"
    for j in cols:
        start+=f"""\n\t,AVG({j}) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS rolling_{roll_number}_avg_{j}"""
    start+=f""",COUNT(*) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS game_count
    

FROM combined.TEAMS_COMBINED
--WHERE SEASON_ID::varchar not like '4%'
ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE"""
    return start

# %%
# print(create_base_team_dataset(columns_wanted, rolling_avg_number))



# %%
conn.execute(create_base_team_dataset(columns_wanted, rolling_avg_number)).df()


with open('creation_team.sql' ,'w') as f:
     f.write(create_base_team_dataset(columns_wanted, rolling_avg_number))

# %%
# first_sample = conn.execute("select * from processed.TEMP_TEAM_10_AVG_DATA order by RANDOM() limit 1000").df()
# first_sample.to_csv('temp/sample_team_first.csv')



# %%
def create_step_2_dataset(cols,roll_number):
    start = f"""
    CREATE OR REPLACE TABLE processed.TEAM_AVG_{roll_number}_TABLE AS
    
    WITH HA_MATCHUPS AS (
        SELECT
            GAME_ID,
            GAME_DATE,
            TEAM_ABBREVIATION,
            MATCHUP,
            CASE 
                WHEN MATCHUP like '%vs.%' THEN SUBSTRING(MATCHUP, 0, 4)
                ELSE SUBSTRING(MATCHUP, 7)
            END AS HOME_TEAM,
            
            CASE 
                WHEN MATCHUP like '%vs.%' THEN SUBSTRING(MATCHUP, 8, 4)
                ELSE SUBSTRING(MATCHUP, 0, 4)
            END AS AWAY_TEAM
        FROM processed.TEMP_TEAM_{roll_number}_AVG_DATA
    )

    SELECT 
        current_game.GAME_ID::int as GAME_ID
        ,current_game.GAME_DATE
        ,current_game.MATCHUP"""
    
    for col in cols:
          start+=f"\n\t,home_team_data.rolling_{roll_number}_avg_{col} AS HOME_{col}"

    start+="\n\t,home_team_data.GAME_COUNT as HOME_GAME_COUNT"
    
    for col2 in cols:
          start+=f"\n\t,away_team_data.rolling_{roll_number}_avg_{col} AS AWAY_{col2}"
    
    start+="\n\t,away_team_data.GAME_COUNT as AWAY_GAME_COUNT"

    start+="\n\t,home_team_data.PLUS_MINUS as PLUS_MINUS"

    start+="\n\t,home_team_data.PTS + away_team_data.PTS as TOTAL_POINTS"

    start+=f"\n\t,lt.line as HOME_SPREAD\n\t,lt.OU as OVER_UNDER"
        
    start+=f"""\nFROM HA_MATCHUPS current_game

    -- Join to get home team data for the next game
    JOIN processed.TEMP_TEAM_{roll_number}_AVG_DATA home_team_data
        ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
        AND current_game.GAME_ID = home_team_data.GAME_ID

    -- Join to get away team data for the next game
    JOIN processed.TEMP_TEAM_{roll_number}_AVG_DATA away_team_data
        ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
        AND current_game.GAME_ID = away_team_data.GAME_ID
    
    JOIN raw.LINES_TABLE lt
        on current_game.HOME_TEAM = lt.TEAM_ABBREVIATION
        and current_game.GAME_ID::int = lt.GAME_ID::int


    ORDER BY 
        current_game.GAME_DATE
    ;"""
    
    return start



# %%
conn.execute(create_step_2_dataset(columns_wanted,rolling_avg_number)).df()

with open('creation_team.sql' ,'a') as f:
     f.write("\n\n"+create_step_2_dataset(columns_wanted,rolling_avg_number))

# %%

# %%
sample = conn.execute(f"""
SELECT * FROM processed.TEAM_AVG_10_TABLE ORDER BY RANDOM() limit 1000
             """).df()

sample.to_csv('temp/team_average_sample.csv')


# %%
# sample = conn.execute(f"""
# SELECT * FROM processed.TEAM_AVG_10_TABLE 
# WHERE HOME_GAME_COUNT > 15 ORDER BY RANDOM() limit 10000
#              """).df()

# sample.to_csv('../modeling/datasets/team_sample.csv')

# %%
conn.close()




