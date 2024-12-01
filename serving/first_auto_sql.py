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
