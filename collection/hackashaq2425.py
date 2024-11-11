
# %%
print('start')


### SEARCH FOR THIS IN EMAIL "ESPN Fantasy Basketball Draft Results"

# %% 
import pandas as pd
import glob
import time
import duckdb

# %%
conn = duckdb.connect('firstdb.db')

# %%
conn.execute(f"""SHOW TABLES""").fetchall()

# %%
columns = conn.execute("SELECT * FROM information_schema.columns WHERE table_name ilike '%players_traditional%'").df()
columns.to_csv('player_cols.csv')
# %%
    # CREATE OR REPLACE TABLE player_fantasy_base as

# new_table =
conn.execute(f"""
    CREATE OR REPLACE TABLE player_fantasy_base as
    SELECT
             t1.PLAYER_NAME,

             case when t2.minutes <> '0'
             then LEFT(t2.minutes, POSITION(':' IN t2.minutes) -1)::int
             else 0 end as minutes,

             t2.points,
             t2.assists,
             t2.steals,
             t2.blocks,
             t2.reboundsTotal as rebounds,
             t2.threePointersMade as THREEPM,

             t1.EFG_PCT,
             case when t2.freeThrowsAttempted = 0 then Null else t2.freeThrowsPercentage end as FT_PCT,
             case when t2.points >= 10
             and (t2.assists >= 10 or t2.blocks >= 10 or t2.reboundsTotal >= 10)
             or t2.assists >= 10
             and (t2.points >= 10 or t2.blocks >= 10 or t2.reboundsTotal >= 10)
             or t2.reboundsTotal >= 10
             and (t2.points >= 10 or t2.blocks >= 10 or t2.assists >= 10)
             then 1
             else 0
             end as DD
             


    FROM
             players_advanced t1
    JOIN
             players_traditional t2
             on t1.GAME_ID = t2.gameId and t1.PLAYER_ID = t2.personId
""").df()

# %%
conn.execute(f"""
    SELECT
             *
    from player_fantasy_base
""").df().to_csv('player_data_fb.csv')
# %%
conn.execute(f"""
    CREATE OR REPLACE TABLE player_averages as
    SELECT
             player_name,
             avg(minutes) as min,
             avg(points) as pts,
             avg(assists) as ast,
             avg(steals) as stl,
             avg(blocks) as blk,
             avg(rebounds) as rb,
             avg(THREEPM) as tpm,
             avg(EFG_PCT) as efgp,
             avg(FT_PCT) as ftp,
             avg(DD) as dd

    FROM
             player_fantasy_base t1
    WHERE minutes <> 0.0
    group by PLAYER_NAME
             order by pts desc
""").df()


# %%
conn.execute(f"""
    SELECT
             *
    from player_averages
""").df().to_csv('fb_avg_data.csv')


# %%
conn.execute(f"""
    SELECT
             player_name,
             avg(minutes) as min,
             avg(points) as pts,
             avg(assists) as ast,
             avg(steals) as stl,
             avg(blocks) as blk,
             avg(rebounds) as rb,
             avg(THREEPM) as tpm,
             avg(EFG_PCT) as efgp,
             avg(FT_PCT) as ftp,
             avg(DD) as dd

    FROM
             player_fantasy_base t1
    WHERE minutes <> 0.0
    group by PLAYER_NAME
             order by pts desc
""").df()

# %%


# %%
conn.execute(f"""
CREATE OR REPLACE TABLE Z_SCORE_TABLE AS
WITH stats AS (
    SELECT
        AVG(min) AS avg_min,
        stddev_pop(min) AS stddev_pop_min,
        AVG(pts) AS avg_pts,
        stddev_pop(pts) AS stddev_pop_pts,
        AVG(ast) AS avg_ast,
        stddev_pop(ast) AS stddev_pop_ast,
        AVG(stl) AS avg_stl,
        stddev_pop(stl) AS stddev_pop_stl,
        AVG(blk) AS avg_blk,
        stddev_pop(blk) AS stddev_pop_blk,
        AVG(rb) AS avg_rb,
        stddev_pop(rb) AS stddev_pop_rb,
        AVG(tpm) AS avg_tpm,
        stddev_pop(tpm) AS stddev_pop_tpm,
        AVG(efgp) AS avg_efgp,
        stddev_pop(efgp) AS stddev_pop_efgp,
        AVG(ftp) AS avg_ftp,
        stddev_pop(ftp) AS stddev_pop_ftp,
        AVG(dd) AS avg_dd,
        stddev_pop(dd) AS stddev_pop_dd
    FROM
        player_averages
)         
SELECT
    t.PLAYER_NAME,
    (t.min - s.avg_min) / s.stddev_pop_min AS z_min,
    (t.pts - s.avg_pts) / s.stddev_pop_pts AS z_pts,
    (t.ast - s.avg_ast) / s.stddev_pop_ast AS z_ast,
    (t.stl - s.avg_stl) / s.stddev_pop_stl AS z_stl,
    (t.blk - s.avg_blk) / s.stddev_pop_blk AS z_blk,
    (t.rb - s.avg_rb) / s.stddev_pop_rb AS z_rb,
    (t.tpm - s.avg_tpm) / s.stddev_pop_tpm AS z_tpm,
    (t.efgp - s.avg_efgp) / s.stddev_pop_efgp AS z_efgp,
    (t.ftp - s.avg_ftp) / s.stddev_pop_ftp AS z_ftp,
    (t.dd - s.avg_dd) / s.stddev_pop_dd AS z_dd,
    z_pts + z_ast + z_stl + z_blk + z_rb + z_tpm + z_efgp + z_ftp + z_dd as total_score
FROM
    player_averages AS t
JOIN
    stats AS s ON 1=1
order by total_score desc
""").df()
# %%
# %%
conn.execute(f"""
    SELECT
    player_name, total_score
    from Z_SCORE_TABLE
""").df().to_csv('total_z_scores.csv') 
# %%

conn.execute(f"""
WITH positive_players AS (
    SELECT
        PLAYER_NAME,
        total_score,
        POWER(total_score, 3) AS weighted_score
    FROM
        Z_SCORE_TABLE
    WHERE
        total_score >= 2
),
total_weighted_scores AS (
    SELECT
        SUM(weighted_score) AS total_weighted_score
    FROM
        positive_players
)
SELECT
    pp.PLAYER_NAME,
    (pp.weighted_score / tws.total_weighted_score) * 6690 AS distributed_amount
FROM
    positive_players pp,
    total_weighted_scores tws;
""").df().to_csv('FANTASY/money_table2.csv')


# %%
## deraft results
with open ('FANTASY/draft.txt','r') as f:
    new = f.readlines()

print(new)
# %%
teams = dict()
newteam = True
for i in new:
    if 'Budget' in i:
        newteam = True
        continue
    if '$' in i:
        continue
    if newteam:
        current_team = i.replace("\n","")
        teams[current_team] = []
        newteam = False
        continue
    else:
        teams[current_team].append(i.replace("\n",""))
print(teams)


# %%
scores = conn.execute(f"""
    SELECT
    player_name, total_score
    from Z_SCORE_TABLE
""").fetchall()
# %%
score_d = dict()
for i in scores:
    score_d[i[0]] = i[1]
# %%
print(score_d)
# %%
totals = dict()
for i in teams:
    totals[i] = 0
    for player in teams[i]:
        try:
            totals[i] += score_d[player]
        except:
            print(f"ROOOK: {player}")
print(totals)
# %%
for team in totals:
    print(f"TEAM: {team}, SCORE: {totals[team]}")
# %%
