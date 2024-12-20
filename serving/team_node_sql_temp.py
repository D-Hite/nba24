"""
extraction datasets for use in ../modeling



"""



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
conn.execute("""
CREATE OR REPLACE TABLE processed.TEAM_AVG_NODE_10_TABLE AS

WITH HA_MATCHUPS AS (
    SELECT
        GAME_ID,
        GAME_DATE,
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
),

combined AS (
    -- Get data for both home and away teams
    SELECT 
        current_game.GAME_ID::int as GAME_ID,
        current_game.GAME_DATE,
        current_game.HOME_TEAM,
        current_game.AWAY_TEAM,
        
        home_team_data.TEAM_ABBREVIATION as TEAM_ABBREVIATION, 
        home_team_data.rolling_10_avg_EFG_PCT AS EFG_PCT,
        home_team_data.rolling_10_avg_FTA_RATE AS FTA_RATE,
        home_team_data.rolling_10_avg_TM_TOV_PCT AS TM_TOV_PCT,
        home_team_data.rolling_10_avg_OREB_PCT AS OREB_PCT,
        home_team_data.rolling_10_avg_OPP_EFG_PCT AS OPP_EFG_PCT,
        home_team_data.rolling_10_avg_OPP_FTA_RATE AS OPP_FTA_RATE,
        home_team_data.rolling_10_avg_OPP_TOV_PCT AS OPP_TOV_PCT,
        home_team_data.rolling_10_avg_OPP_OREB_PCT AS OPP_OREB_PCT,
        home_team_data.GAME_COUNT as GAME_COUNT,
        home_team_data.PLUS_MINUS as PLUS_MINUS,
        home_team_data.PTS + away_team_data.PTS as TOTAL_POINTS,
        lt.line as HOME_SPREAD,
        lt.OU as OVER_UNDER,
        'HOME' as TEAM_TYPE

    FROM HA_MATCHUPS current_game
    
    -- Join to get home team data
    JOIN TEMP_TEAM_10_AVG_DATA home_team_data
        ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
        AND current_game.GAME_ID = home_team_data.GAME_ID

    -- Join to get away team data
    JOIN TEMP_TEAM_10_AVG_DATA away_team_data
        ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
        AND current_game.GAME_ID = away_team_data.GAME_ID
    
    JOIN LINES_TABLE lt
        ON current_game.HOME_TEAM = lt.TEAM_ABBREVIATION
        AND current_game.GAME_ID::int = lt.GAME_ID::int

    UNION ALL

    SELECT
        current_game.GAME_ID::int as GAME_ID,
        current_game.GAME_DATE,
        current_game.HOME_TEAM,
        current_game.AWAY_TEAM,
        
        away_team_data.TEAM_ABBREVIATION as TEAM_ABBREVIATION, 
        away_team_data.rolling_10_avg_EFG_PCT AS EFG_PCT,
        away_team_data.rolling_10_avg_FTA_RATE AS FTA_RATE,
        away_team_data.rolling_10_avg_TM_TOV_PCT AS TM_TOV_PCT,
        away_team_data.rolling_10_avg_OREB_PCT AS OREB_PCT,
        away_team_data.rolling_10_avg_OPP_EFG_PCT AS OPP_EFG_PCT,
        away_team_data.rolling_10_avg_OPP_FTA_RATE AS OPP_FTA_RATE,
        away_team_data.rolling_10_avg_OPP_TOV_PCT AS OPP_TOV_PCT,
        away_team_data.rolling_10_avg_OPP_OREB_PCT AS OPP_OREB_PCT,
        away_team_data.GAME_COUNT as GAME_COUNT,
        away_team_data.PLUS_MINUS as PLUS_MINUS,
        away_team_data.PTS + home_team_data.PTS as TOTAL_POINTS,
        -lt.line as HOME_SPREAD, -- Reverse the spread for away team
        lt.OU as OVER_UNDER,
        'AWAY' as TEAM_TYPE

    FROM HA_MATCHUPS current_game
    
    -- Join to get home team data
    JOIN TEMP_TEAM_10_AVG_DATA home_team_data
        ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
        AND current_game.GAME_ID = home_team_data.GAME_ID

    -- Join to get away team data
    JOIN TEMP_TEAM_10_AVG_DATA away_team_data
        ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
        AND current_game.GAME_ID = away_team_data.GAME_ID
    
    JOIN LINES_TABLE lt
        ON current_game.HOME_TEAM = lt.TEAM_ABBREVIATION
        AND current_game.GAME_ID::int = lt.GAME_ID::int
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY combined.GAME_ID, combined.GAME_DATE, combined.TEAM_ABBREVIATION) - 1 AS TEAM_NODE_ID,  -- Global sequential ID
    combined.GAME_ID,
    combined.GAME_DATE,
    combined.HOME_TEAM,
    combined.AWAY_TEAM,
    combined.TEAM_ABBREVIATION,
    combined.EFG_PCT,
    combined.FTA_RATE,
    combined.TM_TOV_PCT,
    combined.OREB_PCT,
    combined.OPP_EFG_PCT,
    combined.OPP_FTA_RATE,
    combined.OPP_TOV_PCT,
    combined.OPP_OREB_PCT,
    combined.GAME_COUNT,
    combined.PLUS_MINUS,
    combined.TOTAL_POINTS,
    combined.HOME_SPREAD,
    combined.OVER_UNDER,
    combined.TEAM_TYPE
FROM combined
ORDER BY combined.GAME_DATE, combined.TEAM_TYPE;



""").df()



# %%
sample = conn.execute(f"""
WITH games AS (
    SELECT DISTINCT GAME_ID
    FROM TEAM_AVG_NODE_10_TABLE
    WHERE GAME_COUNT > 15
    AND CAST(GAME_DATE as DATE) > CAST('2015-07-01' AS DATE)
    ORDER BY RANDOM()
    LIMIT 10000  -- Retrieve 10,000 unique games
)
SELECT *
FROM TEAM_AVG_NODE_10_TABLE
WHERE GAME_ID IN (SELECT GAME_ID FROM games)
ORDER BY TEAM_NODE_ID;

             """).df()

sample.to_csv('../modeling/datasets/team_sample.csv', index=False)
# %%
player_sample = conn.execute(f"""
-- Load the CSV file first as a temporary table
CREATE OR REPLACE TEMPORARY processed.TABLE team_sample AS
SELECT * FROM read_csv_auto('../modeling/datasets/team_sample.csv');

-- Query to fetch data from PLAYER_10_AVG_TABLE where GAME_ID matches from the CSV
SELECT 
    t.TEAM_NODE_ID,
    ROW_NUMBER() OVER (PARTITION BY 0) - 1 AS PLAYER_NODE_ID,  -- Global sequential ID
    p.*
    -- Join with the team data to get the corresponding node_id
    
FROM 
    PLAYER_10_AVG_TABLE p
inner JOIN 
    team_sample t
    ON p.GAME_ID::int = t.GAME_ID::int
    AND p.TEAM_ABBREVIATION = t.TEAM_ABBREVIATION
ORDER BY 
    PLAYER_NODE_ID;

             """).df()



# %%
player_sample.to_csv('../modeling/datasets/player_sample.csv', index=False)

# %%
