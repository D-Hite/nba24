# %%
import pandas as pd
import glob
import time
import duckdb
from os import path



# %%
# conn = duckdb.connect('../collection/firstdb.db')
# %%
basepath = path.dirname(__file__)
filepath = path.abspath(path.join(basepath, "..", "collection", "firstdb.db"))

print(filepath)


# %%
conn = duckdb.connect(filepath)

# %%
conn.execute('SHOW TABLES').df()

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
    start = f"""CREATE OR REPLACE TABLE TEMP_TEAM_{roll_number}_AVG_DATA AS
        SELECT
        TEAM_ABBREVIATION
        ,SEASON_ID
        ,GAME_ID
        ,GAME_DATE
        ,MATCHUP"""
    for i in cols:
        start+=f"\n\t,{i}"
    for j in cols:
        start+=f"""\n\t,AVG({j}) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND CURRENT ROW
    ) AS rolling_{roll_number}_avg_{j}"""
    start+=f""",COUNT(*) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS game_count

    -- Get the NEXT_GAME_ID using the LEAD() function
    ,LEAD(GAME_ID) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
    ) AS NEXT_GAME_ID

FROM TEAMS_COMBINED
WHERE SEASON_ID::varchar not like '4%'
ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE"""
    return start

# %%
print(create_base_team_dataset(columns_wanted, rolling_avg_number))



# %%
conn.execute(create_base_team_dataset(columns_wanted, rolling_avg_number)).df()


# %%
first_sample = conn.execute("select * from TEMP_TEAM_10_AVG_DATA order by RANDOM() limit 1000").df()
first_sample.to_csv('out/first_sample.csv')



# %%
def create_step_2_dataset(cols,roll_number):
    start = f"""
    CREATE OR REPLACE TABLE TEAM_AVG_{roll_number}_TABLE AS
    
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
        FROM TEMP_TEAM_{roll_number}_AVG_DATA
    )

    SELECT 
        current_game.GAME_ID
        ,current_game.GAME_DATE
        ,current_game.MATCHUP"""
    
    for col in cols:
          start+=f"\n\t,home_team_data.{col} AS HOME_{col}"

    start+="\n\t,home_team_data.GAME_COUNT as HOME_GAME_COUNT"
    
    for col2 in cols:
          start+=f"\n\t,away_team_data.{col2} AS AWAY_{col2}"
    
    start+="\n\t,away_team_data.GAME_COUNT as AWAY_GAME_COUNT"

    start+="\n\t,current_game.PLUS_MINUS as PLUS_MINUS"
        
    start+=f"""\nFROM HA_MATCHUPS current_game

    -- Join to get home team data for the next game
    JOIN TEMP_TEAM_{roll_number}_AVG_DATA home_team_data
        ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
        AND current_game.GAME_ID = home_team_data.NEXT_GAME_ID

    -- Join to get away team data for the next game
    JOIN TEMP_TEAM_{roll_number}_AVG_DATA away_team_data
        ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
        AND current_game.GAME_ID = away_team_data.NEXT_GAME_ID

    ORDER BY 
        current_game.GAME_DATE
    ;"""
    
    return start



# %%
print(create_step_2_dataset(columns_wanted,rolling_avg_number))


# %%
conn.execute(create_step_2_dataset(columns_wanted,rolling_avg_number)).df()

# %%

# %%
sample = conn.execute(f"""
SELECT * FROM TEAM_AVG_10_TABLE ORDER BY RANDOM() limit 1000
             """).df()

sample.to_csv('out/second_sample.csv')
























































# %%
conn.execute(f"""
with HA_MATCHUPS as (
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
from TEMP_TEAM_10_AVG_DATA
)
             select *
             from HA_MATCHUPS
             limit 10


             """).df()



# %%
conn.execute(f"""WITH HA_MATCHUPS AS (
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
    FROM TEMP_TEAM_10_AVG_DATA
)

SELECT 
    current_game.GAME_ID,
    current_game.GAME_DATE,
    current_game.MATCHUP,
    
    -- Home team data for the next game
    home_team_data.EFG_PCT AS HOME_EFG_PCT,
    home_team_data.FTA_RATE AS HOME_FTA_RATE,
    home_team_data.TM_TOV_PCT AS HOME_TM_TOV_PCT,
    home_team_data.OREB_PCT AS HOME_OREB_PCT,
    home_team_data.OPP_EFG_PCT AS HOME_OPP_EFG_PCT,
    home_team_data.OPP_FTA_RATE AS HOME_OPP_FTA_RATE,
    home_team_data.OPP_TOV_PCT AS HOME_OPP_TOV_PCT,
    home_team_data.OPP_OREB_PCT AS HOME_OPP_OREB_PCT,
    
    -- Away team data for the next game
    away_team_data.EFG_PCT AS AWAY_EFG_PCT,
    away_team_data.FTA_RATE AS AWAY_FTA_RATE,
    away_team_data.TM_TOV_PCT AS AWAY_TM_TOV_PCT,
    away_team_data.OREB_PCT AS AWAY_OREB_PCT,
    away_team_data.OPP_EFG_PCT AS AWAY_OPP_EFG_PCT,
    away_team_data.OPP_FTA_RATE AS AWAY_OPP_FTA_RATE,
    away_team_data.OPP_TOV_PCT AS AWAY_OPP_TOV_PCT,
    away_team_data.OPP_OREB_PCT AS AWAY_OPP_OREB_PCT

FROM 
    HA_MATCHUPS current_game

-- Join to get home team data for the next game
JOIN TEMP_TEAM_10_AVG_DATA home_team_data
    ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
    AND current_game.GAME_ID = home_team_data.NEXT_GAME_ID

-- Join to get away team data for the next game
JOIN TEMP_TEAM_10_AVG_DATA away_team_data
    ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
    AND current_game.GAME_ID = away_team_data.NEXT_GAME_ID

ORDER BY 
    current_game.GAME_DATE
             
             
             limit 100;
""").df()



# %%
# test 300


conn.execute("""
select *
             from TEMP_TEAM_10_AVG_DATA
             where GAME_ID = 0021000115
             or NEXT_GAME_ID = 0021000115




""").df()

























# %%





first_sample = conn.execute(f"""
SELECT
    TEAM_ABBREVIATION,
    SEASON_ID,
    GAME_ID,
    GAME_DATE,
    EFG_PCT,
    FTA_RATE,
    TM_TOV_PCT,
    OREB_PCT,
    OPP_EFG_PCT,
    OPP_FTA_RATE,
    OPP_TOV_PCT,
    OPP_OREB_PCT,
    
    -- Calculate rolling averages (adjust window size as needed, here we use 5 games)
    AVG(EFG_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_EFG_PCT,
    
    AVG(FTA_RATE) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_FTA_RATE,
    
    AVG(TM_TOV_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_TM_TOV_PCT,
    
    AVG(OREB_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_OREB_PCT,
    
    AVG(OPP_EFG_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_OPP_EFG_PCT,
    
    AVG(OPP_FTA_RATE) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_OPP_FTA_RATE,
    
    AVG(OPP_TOV_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_OPP_TOV_PCT,
    
    AVG(OPP_OREB_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {rolling_avg_number} PRECEDING AND CURRENT ROW
    ) AS rolling_avg_OPP_OREB_PCT,
    
    -- Calculate the total number of games played in the season up to the current game
    COUNT(*) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS game_count

FROM TEAMS_COMBINED
WHERE SEASON_ID::varchar not like '4%'
ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE         
             
             ;
"""
).df()




# %%

first_sample.to_csv('out/first_sample.csv')




# %%
conn.close()
# %%
