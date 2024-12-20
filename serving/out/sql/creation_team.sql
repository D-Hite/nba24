CREATE OR REPLACE TABLE processed.TEMP_TEAM_10_AVG_DATA AS
        SELECT
        TEAM_ABBREVIATION
        ,SEASON_ID
        ,GAME_ID
        ,GAME_DATE
        ,MATCHUP
        ,PLUS_MINUS
        ,PTS
	,EFG_PCT
	,FTA_RATE
	,TM_TOV_PCT
	,OREB_PCT
	,OPP_EFG_PCT
	,OPP_FTA_RATE
	,OPP_TOV_PCT
	,OPP_OREB_PCT
	,AVG(EFG_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_EFG_PCT
	,AVG(FTA_RATE) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_FTA_RATE
	,AVG(TM_TOV_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_TM_TOV_PCT
	,AVG(OREB_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_OREB_PCT
	,AVG(OPP_EFG_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_OPP_EFG_PCT
	,AVG(OPP_FTA_RATE) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_OPP_FTA_RATE
	,AVG(OPP_TOV_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_OPP_TOV_PCT
	,AVG(OPP_OREB_PCT) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_10_avg_OPP_OREB_PCT,COUNT(*) OVER (
        PARTITION BY TEAM_ABBREVIATION, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS game_count
    

FROM combined.TEAMS_COMBINED
--WHERE SEASON_ID::varchar not like '4%'
ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE


    CREATE OR REPLACE TABLE processed.TEAM_AVG_10_TABLE AS
    
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
        FROM processed.TEMP_TEAM_10_AVG_DATA
    )

    SELECT 
        current_game.GAME_ID::int as GAME_ID
        ,current_game.GAME_DATE
        ,current_game.MATCHUP
	,home_team_data.rolling_10_avg_EFG_PCT AS HOME_EFG_PCT
	,home_team_data.rolling_10_avg_FTA_RATE AS HOME_FTA_RATE
	,home_team_data.rolling_10_avg_TM_TOV_PCT AS HOME_TM_TOV_PCT
	,home_team_data.rolling_10_avg_OREB_PCT AS HOME_OREB_PCT
	,home_team_data.rolling_10_avg_OPP_EFG_PCT AS HOME_OPP_EFG_PCT
	,home_team_data.rolling_10_avg_OPP_FTA_RATE AS HOME_OPP_FTA_RATE
	,home_team_data.rolling_10_avg_OPP_TOV_PCT AS HOME_OPP_TOV_PCT
	,home_team_data.rolling_10_avg_OPP_OREB_PCT AS HOME_OPP_OREB_PCT
	,home_team_data.GAME_COUNT as HOME_GAME_COUNT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_EFG_PCT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_FTA_RATE
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_TM_TOV_PCT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_OREB_PCT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_OPP_EFG_PCT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_OPP_FTA_RATE
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_OPP_TOV_PCT
	,away_team_data.rolling_10_avg_OPP_OREB_PCT AS AWAY_OPP_OREB_PCT
	,away_team_data.GAME_COUNT as AWAY_GAME_COUNT
	,home_team_data.PLUS_MINUS as PLUS_MINUS
	,home_team_data.PTS + away_team_data.PTS as TOTAL_POINTS
	,lt.line as HOME_SPREAD
	,lt.OU as OVER_UNDER
FROM HA_MATCHUPS current_game

    -- Join to get home team data for the next game
    JOIN processed.TEMP_TEAM_10_AVG_DATA home_team_data
        ON home_team_data.TEAM_ABBREVIATION = current_game.HOME_TEAM
        AND current_game.GAME_ID = home_team_data.GAME_ID

    -- Join to get away team data for the next game
    JOIN processed.TEMP_TEAM_10_AVG_DATA away_team_data
        ON away_team_data.TEAM_ABBREVIATION = current_game.AWAY_TEAM
        AND current_game.GAME_ID = away_team_data.GAME_ID
    
    JOIN raw.LINES_TABLE lt
        on current_game.HOME_TEAM = lt.TEAM_ABBREVIATION
        and current_game.GAME_ID::int = lt.GAME_ID::int


    ORDER BY 
        current_game.GAME_DATE
    ;