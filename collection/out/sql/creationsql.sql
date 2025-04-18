TEAMS:
CREATE OR REPLACE TABLE combined.teams_combined as
            SELECT DISTINCT
            
	raw.log_table.SEASON_ID,
	COALESCE(raw.log_table.TEAM_ID,raw.teams_fourfactors.TEAM_ID,raw.teams_advanced.TEAM_ID,raw.teams_misc.TEAM_ID,raw.teams_scoring.TEAM_ID,raw.teams_traditional.TEAM_ID) as TEAM_ID,
	COALESCE(raw.log_table.TEAM_ABBREVIATION,raw.teams_fourfactors.TEAM_ABBREVIATION,raw.lines_table.TEAM_ABBREVIATION,raw.teams_advanced.TEAM_ABBREVIATION,raw.teams_misc.TEAM_ABBREVIATION,raw.teams_scoring.TEAM_ABBREVIATION,raw.teams_traditional.TEAM_ABBREVIATION) as TEAM_ABBREVIATION,
	COALESCE(raw.log_table.TEAM_NAME,raw.teams_fourfactors.TEAM_NAME,raw.teams_advanced.TEAM_NAME,raw.teams_misc.TEAM_NAME,raw.teams_scoring.TEAM_NAME,raw.teams_traditional.TEAM_NAME) as TEAM_NAME,
	COALESCE(CAST(raw.log_table.GAME_ID AS VARCHAR),CAST(raw.teams_fourfactors.GAME_ID AS VARCHAR),CAST(raw.lines_table.GAME_ID AS VARCHAR),CAST(raw.teams_advanced.GAME_ID AS VARCHAR),CAST(raw.teams_misc.GAME_ID AS VARCHAR),CAST(raw.teams_scoring.GAME_ID AS VARCHAR),CAST(raw.teams_traditional.GAME_ID AS VARCHAR)) as GAME_ID,
	COALESCE(raw.log_table.GAME_DATE,raw.lines_table.GAME_DATE) as GAME_DATE,
	raw.log_table.MATCHUP,
	raw.log_table.WL,
	COALESCE(CAST(raw.log_table.MIN AS VARCHAR),CAST(raw.teams_fourfactors.MIN AS VARCHAR),CAST(raw.teams_advanced.MIN AS VARCHAR),CAST(raw.teams_misc.MIN AS VARCHAR),CAST(raw.teams_scoring.MIN AS VARCHAR),CAST(raw.teams_traditional.MIN AS VARCHAR)) as MIN,
	COALESCE(raw.log_table.PTS,raw.teams_traditional.PTS) as PTS,
	COALESCE(raw.log_table.FGM,raw.teams_traditional.FGM) as FGM,
	COALESCE(raw.log_table.FGA,raw.teams_traditional.FGA) as FGA,
	COALESCE(raw.log_table.FG_PCT,raw.teams_traditional.FG_PCT) as FG_PCT,
	COALESCE(raw.log_table.FG3M,raw.teams_traditional.FG3M) as FG3M,
	COALESCE(raw.log_table.FG3A,raw.teams_traditional.FG3A) as FG3A,
	COALESCE(raw.log_table.FG3_PCT,raw.teams_traditional.FG3_PCT) as FG3_PCT,
	COALESCE(raw.log_table.FTM,raw.teams_traditional.FTM) as FTM,
	COALESCE(raw.log_table.FTA,raw.teams_traditional.FTA) as FTA,
	COALESCE(raw.log_table.FT_PCT,raw.teams_traditional.FT_PCT) as FT_PCT,
	COALESCE(raw.log_table.OREB,raw.teams_traditional.OREB) as OREB,
	COALESCE(raw.log_table.DREB,raw.teams_traditional.DREB) as DREB,
	COALESCE(raw.log_table.REB,raw.teams_traditional.REB) as REB,
	COALESCE(raw.log_table.AST,raw.teams_traditional.AST) as AST,
	COALESCE(raw.log_table.STL,raw.teams_traditional.STL) as STL,
	COALESCE(raw.log_table.BLK,raw.teams_misc.BLK,raw.teams_traditional.BLK) as BLK,
	raw.log_table.TOV,
	COALESCE(raw.log_table.PF,raw.teams_misc.PF,raw.teams_traditional.PF) as PF,
	COALESCE(raw.log_table.PLUS_MINUS,raw.teams_traditional.PLUS_MINUS) as PLUS_MINUS,
	raw.lines_table.LINE,
	raw.lines_table.OU,
	COALESCE(raw.teams_fourfactors.TEAM_CITY,raw.teams_advanced.TEAM_CITY,raw.teams_misc.TEAM_CITY,raw.teams_scoring.TEAM_CITY,raw.teams_traditional.TEAM_CITY) as TEAM_CITY,
	raw.teams_advanced.E_OFF_RATING,
	raw.teams_advanced.OFF_RATING,
	raw.teams_advanced.E_DEF_RATING,
	raw.teams_advanced.DEF_RATING,
	raw.teams_advanced.E_NET_RATING,
	raw.teams_advanced.NET_RATING,
	raw.teams_advanced.AST_PCT,
	raw.teams_advanced.AST_TOV,
	raw.teams_advanced.AST_RATIO,
	COALESCE(raw.teams_fourfactors.OREB_PCT,raw.teams_advanced.OREB_PCT) as OREB_PCT,
	raw.teams_advanced.DREB_PCT,
	raw.teams_advanced.REB_PCT,
	raw.teams_advanced.E_TM_TOV_PCT,
	COALESCE(raw.teams_fourfactors.TM_TOV_PCT,raw.teams_advanced.TM_TOV_PCT) as TM_TOV_PCT,
	COALESCE(raw.teams_fourfactors.EFG_PCT,raw.teams_advanced.EFG_PCT) as EFG_PCT,
	raw.teams_advanced.TS_PCT,
	raw.teams_advanced.USG_PCT,
	raw.teams_advanced.E_USG_PCT,
	raw.teams_advanced.E_PACE,
	raw.teams_advanced.PACE,
	raw.teams_advanced.PACE_PER40,
	raw.teams_advanced.POSS,
	raw.teams_advanced.PIE,
	raw.teams_fourfactors.FTA_RATE,
	raw.teams_fourfactors.OPP_EFG_PCT,
	raw.teams_fourfactors.OPP_FTA_RATE,
	raw.teams_fourfactors.OPP_TOV_PCT,
	raw.teams_fourfactors.OPP_OREB_PCT,
	raw.teams_misc.PTS_OFF_TOV,
	raw.teams_misc.PTS_2ND_CHANCE,
	raw.teams_misc.PTS_FB,
	raw.teams_misc.PTS_PAINT,
	raw.teams_misc.OPP_PTS_OFF_TOV,
	raw.teams_misc.OPP_PTS_2ND_CHANCE,
	raw.teams_misc.OPP_PTS_FB,
	raw.teams_misc.OPP_PTS_PAINT,
	raw.teams_misc.BLKA,
	raw.teams_misc.PFD,
	raw.teams_scoring.PCT_FGA_2PT,
	raw.teams_scoring.PCT_FGA_3PT,
	raw.teams_scoring.PCT_PTS_2PT,
	raw.teams_scoring.PCT_PTS_2PT_MR,
	raw.teams_scoring.PCT_PTS_3PT,
	raw.teams_scoring.PCT_PTS_FB,
	raw.teams_scoring.PCT_PTS_FT,
	raw.teams_scoring.PCT_PTS_OFF_TOV,
	raw.teams_scoring.PCT_PTS_PAINT,
	raw.teams_scoring.PCT_AST_2PM,
	raw.teams_scoring.PCT_UAST_2PM,
	raw.teams_scoring.PCT_AST_3PM,
	raw.teams_scoring.PCT_UAST_3PM,
	raw.teams_scoring.PCT_AST_FGM,
	raw.teams_scoring.PCT_UAST_FGM,
	raw.teams_traditional.TO,
        FROM raw.log_table
left join raw.teams_misc on raw.log_table.GAME_ID::int = raw.teams_misc.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.teams_misc.TEAM_ABBREVIATION
left join raw.teams_traditional on raw.log_table.GAME_ID::int = raw.teams_traditional.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.teams_traditional.TEAM_ABBREVIATION
left join raw.teams_advanced on raw.log_table.GAME_ID::int = raw.teams_advanced.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.teams_advanced.TEAM_ABBREVIATION
left join raw.lines_table on raw.log_table.GAME_ID::int = raw.lines_table.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.lines_table.TEAM_ABBREVIATION
left join raw.teams_fourfactors on raw.log_table.GAME_ID::int = raw.teams_fourfactors.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.teams_fourfactors.TEAM_ABBREVIATION
left join raw.teams_scoring on raw.log_table.GAME_ID::int = raw.teams_scoring.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.teams_scoring.TEAM_ABBREVIATION

PLAYERS:
CREATE OR REPLACE TABLE combined.players_combined as
            SELECT DISTINCT
            
	raw.log_table.SEASON_ID,
	raw.log_table.TEAM_ID,
	raw.log_table.TEAM_ABBREVIATION,
	raw.log_table.TEAM_NAME,
	raw.log_table.GAME_ID,
	raw.log_table.GAME_DATE,
	raw.log_table.MATCHUP,
	raw.log_table.WL,
	COALESCE(raw.players_fourfactors.MIN,raw.players_advanced.MIN,raw.players_misc.MIN,raw.players_scoring.MIN,raw.players_traditional.MIN) as MIN,
	raw.players_traditional.PTS,
	raw.players_traditional.FGM,
	raw.players_traditional.FGA,
	raw.players_traditional.FG_PCT,
	raw.players_traditional.FG3M,
	raw.players_traditional.FG3A,
	raw.players_traditional.FG3_PCT,
	raw.players_traditional.FTM,
	raw.players_traditional.FTA,
	raw.players_traditional.FT_PCT,
	raw.players_traditional.OREB,
	raw.players_traditional.DREB,
	raw.players_traditional.REB,
	raw.players_traditional.AST,
	raw.players_traditional.STL,
	COALESCE(raw.players_misc.BLK,raw.players_traditional.BLK) as BLK,
	COALESCE(raw.players_misc.PF,raw.players_traditional.PF) as PF,
	raw.players_traditional.PLUS_MINUS,
	COALESCE(raw.players_fourfactors.TEAM_CITY,raw.players_advanced.TEAM_CITY,raw.players_misc.TEAM_CITY,raw.players_scoring.TEAM_CITY,raw.players_traditional.TEAM_CITY) as TEAM_CITY,
	COALESCE(raw.players_fourfactors.PLAYER_ID,raw.players_advanced.PLAYER_ID,raw.players_misc.PLAYER_ID,raw.players_scoring.PLAYER_ID,raw.players_traditional.PLAYER_ID) as PLAYER_ID,
	COALESCE(raw.players_fourfactors.PLAYER_NAME,raw.players_advanced.PLAYER_NAME,raw.players_misc.PLAYER_NAME,raw.players_scoring.PLAYER_NAME,raw.players_traditional.PLAYER_NAME) as PLAYER_NAME,
	COALESCE(raw.players_fourfactors.NICKNAME,raw.players_advanced.NICKNAME,raw.players_misc.NICKNAME,raw.players_scoring.NICKNAME,raw.players_traditional.NICKNAME) as NICKNAME,
	COALESCE(raw.players_fourfactors.START_POSITION,raw.players_advanced.START_POSITION,raw.players_misc.START_POSITION,raw.players_scoring.START_POSITION,raw.players_traditional.START_POSITION) as START_POSITION,
	COALESCE(raw.players_fourfactors.COMMENT,raw.players_advanced.COMMENT,raw.players_misc.COMMENT,raw.players_scoring.COMMENT,raw.players_traditional.COMMENT) as COMMENT,
	raw.players_advanced.E_OFF_RATING,
	raw.players_advanced.OFF_RATING,
	raw.players_advanced.E_DEF_RATING,
	raw.players_advanced.DEF_RATING,
	raw.players_advanced.E_NET_RATING,
	raw.players_advanced.NET_RATING,
	raw.players_advanced.AST_PCT,
	raw.players_advanced.AST_TOV,
	raw.players_advanced.AST_RATIO,
	COALESCE(raw.players_fourfactors.OREB_PCT,raw.players_advanced.OREB_PCT) as OREB_PCT,
	raw.players_advanced.DREB_PCT,
	raw.players_advanced.REB_PCT,
	COALESCE(raw.players_fourfactors.TM_TOV_PCT,raw.players_advanced.TM_TOV_PCT) as TM_TOV_PCT,
	COALESCE(raw.players_fourfactors.EFG_PCT,raw.players_advanced.EFG_PCT) as EFG_PCT,
	raw.players_advanced.TS_PCT,
	raw.players_advanced.USG_PCT,
	raw.players_advanced.E_USG_PCT,
	raw.players_advanced.E_PACE,
	raw.players_advanced.PACE,
	raw.players_advanced.PACE_PER40,
	raw.players_advanced.POSS,
	raw.players_advanced.PIE,
	raw.players_fourfactors.FTA_RATE,
	raw.players_fourfactors.OPP_EFG_PCT,
	raw.players_fourfactors.OPP_FTA_RATE,
	raw.players_fourfactors.OPP_TOV_PCT,
	raw.players_fourfactors.OPP_OREB_PCT,
	raw.players_misc.PTS_OFF_TOV,
	raw.players_misc.PTS_2ND_CHANCE,
	raw.players_misc.PTS_FB,
	raw.players_misc.PTS_PAINT,
	raw.players_misc.OPP_PTS_OFF_TOV,
	raw.players_misc.OPP_PTS_2ND_CHANCE,
	raw.players_misc.OPP_PTS_FB,
	raw.players_misc.OPP_PTS_PAINT,
	raw.players_misc.BLKA,
	raw.players_misc.PFD,
	raw.players_scoring.PCT_FGA_2PT,
	raw.players_scoring.PCT_FGA_3PT,
	raw.players_scoring.PCT_PTS_2PT,
	raw.players_scoring.PCT_PTS_2PT_MR,
	raw.players_scoring.PCT_PTS_3PT,
	raw.players_scoring.PCT_PTS_FB,
	raw.players_scoring.PCT_PTS_FT,
	raw.players_scoring.PCT_PTS_OFF_TOV,
	raw.players_scoring.PCT_PTS_PAINT,
	raw.players_scoring.PCT_AST_2PM,
	raw.players_scoring.PCT_UAST_2PM,
	raw.players_scoring.PCT_AST_3PM,
	raw.players_scoring.PCT_UAST_3PM,
	raw.players_scoring.PCT_AST_FGM,
	raw.players_scoring.PCT_UAST_FGM,
	raw.players_traditional.TO,
        FROM raw.log_table
left join raw.players_traditional on raw.log_table.GAME_ID::int = raw.players_traditional.GAME_ID::int and raw.log_table.TEAM_ABBREVIATION = raw.players_traditional.TEAM_ABBREVIATION
left join raw.players_fourfactors on raw.players_traditional.GAME_ID::int = raw.players_fourfactors.GAME_ID::int and raw.players_traditional.PLAYER_NAME = raw.players_fourfactors.PLAYER_NAME
left join raw.players_scoring on raw.players_fourfactors.GAME_ID::int = raw.players_scoring.GAME_ID::int and raw.players_fourfactors.PLAYER_NAME = raw.players_scoring.PLAYER_NAME
left join raw.players_advanced on raw.players_scoring.GAME_ID::int = raw.players_advanced.GAME_ID::int and raw.players_scoring.PLAYER_NAME = raw.players_advanced.PLAYER_NAME
left join raw.players_misc on raw.players_advanced.GAME_ID::int = raw.players_misc.GAME_ID::int and raw.players_advanced.PLAYER_NAME = raw.players_misc.PLAYER_NAME

