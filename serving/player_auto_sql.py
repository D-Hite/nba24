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
rolling_avg_number = 10
columns_wanted = ['EFG_PCT',
    'FTA_RATE',
    'TM_TOV_PCT',
    'OREB_PCT',
    'OPP_EFG_PCT',
    'OPP_FTA_RATE',
    'OPP_TOV_PCT',
    'OPP_OREB_PCT',
    'USG_PCT']



# %%

def create_player_dataset(cols, roll_number):
    start = f"""CREATE OR REPLACE TABLE processed.PLAYER_{roll_number}_AVG_TABLE AS
    with INTERMEDIATE_DATA as (
        SELECT
        TEAM_ABBREVIATION
        ,PLAYER_NAME
        ,SEASON_ID
        ,GAME_ID
        ,GAME_DATE
        ,MATCHUP"""
    start+=""",CASE WHEN MIN IS NULL THEN 0
    else CAST(SPLIT_PART(MIN, ':', 1) as DOUBLE) + (CAST(SPLIT_PART(MIN, ':', 2) as DOUBLE)) / 60
    end as converted_minutes"""
    start+=f"""\n\t,AVG(converted_minutes) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS rolling_{roll_number}_avg_minutes"""
    for i in cols:
        start+=f"\n\t,{i}"
    for j in cols:
        start+=f"""\n\t,AVG(COALESCE({j},0)) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS rolling_{roll_number}_avg_{j}"""
    start+=f""",COUNT(*) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS GAME_COUNT
    

    FROM combined.PLAYERS_COMBINED
    )

    SELECT 
    TEAM_ABBREVIATION
    ,PLAYER_NAME
    ,SEASON_ID
    ,GAME_ID
    ,GAME_DATE
    ,MATCHUP"""
    start+=f"""\n\t,rolling_{roll_number}_avg_minutes as MINUTES"""
    for j in cols:
        start+=f"""\n\t,rolling_{roll_number}_avg_{j} as {j}"""
    start+="""
    ,GAME_COUNT
    FROM INTERMEDIATE_DATA
    --WHERE SEASON_ID::varchar not like '4%'
    ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE"""
    return start

# %%
print(create_player_dataset(columns_wanted, rolling_avg_number))



# %%
conn.execute(create_player_dataset(columns_wanted, rolling_avg_number)).df()


with open('./out/sql/creation_player.sql' ,'w') as f:
     f.write(create_player_dataset(columns_wanted, rolling_avg_number))

# %%
first_sample = conn.execute("select * from processed.PLAYER_10_AVG_TABLE order by RANDOM() limit 1000").df()
first_sample.to_csv('./out/data/player_l10_avg_sample.csv')


# # %%
# out_sample = conn.execute("select * from PLAYER_10_AVG_TABLE").df()
# out_sample.to_csv('../modeling/datasets/player_sample.csv')

# %%
conn.execute("""select 
             *
             from processed.PLAYER_10_AVG_TABLE 
             where minutes > 20
             and game_count > 10
             and usg_pct > .20
             and SEASON_ID::varchar not like '4%'
             order by EFG_PCT desc
             limit 20
             
             """).df()



# per minute data
# %%
rolling_avg_number = 10
per_minute_columns_wanted = ['PTS',
                            'REB',
                            'AST',
                            'STL',
                            'BLK',
                            'TO']

not_per_minute_columns_wanted = ['PACE',
                                'FG_PCT',
                                'FT_PCT',
                                'PLUS_MINUS']



# %%

def create_player_per_minute_dataset(pmcols, cols, roll_number):
    start = f"""CREATE OR REPLACE TABLE processed.PLAYER_{roll_number}_PER_MIN_AVG_DATA AS
    with INTERMEDIATE_DATA as (
        SELECT
        TEAM_ABBREVIATION
        ,PLAYER_NAME
        ,SEASON_ID
        ,GAME_ID
        ,GAME_DATE
        ,MATCHUP"""
    start+=""",CASE WHEN MIN IS NULL THEN 0
    else CAST(SPLIT_PART(MIN, ':', 1) as DOUBLE) + (CAST(SPLIT_PART(MIN, ':', 2) as DOUBLE)) / 60
    end as converted_minutes"""
    start+=f"""\n\t,AVG(converted_minutes) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS rolling_{roll_number}_avg_minutes"""
    for i in cols:
        start+=f"\n\t,{i}"
    for j in pmcols:
        start+=f"""\n\t,AVG(COALESCE("{j}",0) / converted_minutes) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS avg_{roll_number}_{j}_per_minute"""
    for j2 in cols:
        start+=f"""\n\t,AVG(COALESCE({j2},0)) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID 
        ORDER BY GAME_DATE
        ROWS BETWEEN {roll_number} PRECEDING AND 1 PRECEDING
    ) AS avg_{roll_number}_{j2}"""
    start+=f""",COUNT(*) OVER (
        PARTITION BY PLAYER_NAME, SEASON_ID
        ORDER BY GAME_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS GAME_COUNT
    

    FROM combined.PLAYERS_COMBINED
    )

    SELECT 
    TEAM_ABBREVIATION
    ,PLAYER_NAME
    ,SEASON_ID
    ,GAME_ID
    ,GAME_DATE
    ,MATCHUP"""
    start+=f"""\n\t,rolling_{roll_number}_avg_minutes as MINUTES"""
    for j in pmcols:
        start+=f"""\n\t,round(avg_{roll_number}_{j}_per_minute,3) as AVG_{j}_PM"""
    for j2 in cols:
        start+=f"""\n\t,round(avg_{roll_number}_{j2},3) as AVG_{j2}"""
    start+="""
    ,GAME_COUNT
    FROM INTERMEDIATE_DATA
    --WHERE SEASON_ID::varchar not like '4%'
    ORDER BY SEASON_ID, TEAM_ABBREVIATION, GAME_DATE"""
    return start

# %%
print(create_player_per_minute_dataset(per_minute_columns_wanted,not_per_minute_columns_wanted, rolling_avg_number))



# %%
conn.execute(create_player_per_minute_dataset(per_minute_columns_wanted,not_per_minute_columns_wanted, rolling_avg_number)).df()


with open('./out/sql/creation_player.sql' ,'a') as f:
     f.write('\n\n'+create_player_per_minute_dataset(per_minute_columns_wanted,not_per_minute_columns_wanted, rolling_avg_number))

# %%
first_sample = conn.execute("select * from processed.PLAYER_10_PER_MIN_AVG_DATA order by RANDOM() limit 1000").df()
first_sample.to_csv('./out/data/playerpermin_sample.csv')



# %%
# x = 
conn.execute("""select 
             *
             from processed.PLAYER_10_PER_MIN_AVG_DATA 
             where minutes > 20
             and game_count > 10
             and SEASON_ID::varchar not like '4%'
             and not isnan(AVG_PTS_PM)
             order by AVG_PTS_PM desc
             limit 20
             
             """).df()

# x.to_csv('out/data/player_sample.csv')





# %%
conn.close()





# %%
