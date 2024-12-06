
# %%
print('start')

# %% 
import pandas as pd
import glob
import time
import duckdb

# %%
conn = duckdb.connect('firstdb.db')
current_tables = ['advanced', 'fourfactors', 'misc', 'scoring', 'traditional']

# %%
# conn.close()

# %%
## log data
log_csv_files = glob.glob(f'DATA/raw/log/*.csv')

conn.execute(f"""
    CREATE OR REPLACE TABLE log_table as
    SELECT *
    FROM read_csv_auto('{log_csv_files[0]}')
""").df()

for file in log_csv_files[1:]:
    conn.execute(f"""
        INSERT INTO log_table
        SELECT * FROM read_csv_auto('{file}')
        """)
# %%
## line data
lines_csv_files = glob.glob(f'DATA/lines/*.csv')
# print(lines_csv_files)


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

# %%
## game data
for tp in ['teams', 'players']:
    for kind in current_tables:
        try:
            csv_files = glob.glob(f'DATA/raw/{tp}/{kind}/*.csv')

            conn.execute(f"""
                CREATE OR REPLACE TABLE {tp}_{kind} as
                SELECT *
                FROM read_csv_auto('{csv_files[0]}'
                )
            """).df()
        except Exception as e:
            print(f"problem with {tp} {kind}\n{e}")


        for file in csv_files[1:]:
            conn.execute(f"""
                INSERT INTO {tp}_{kind}
                SELECT *
                FROM read_csv_auto('{file}'
                )
                """)
            
### todo: combine tables, processed data, dynamic query for model
# %%
conn.execute(f"""SHOW TABLES""").fetchall()

# %%
conn.execute("SELECT * FROM information_schema.columns WHERE table_name ilike '%teams_advanced%'").df()

# %%
player_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%'").df()
log_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 'log_table'").df()

# %%
print(player_columns)



# %%
colset = set()
col_dict = dict()
for tn, cn, in zip(player_columns['table_name'],player_columns['column_name']):
    if cn in colset:
        # print('already have col')
        continue
    if tn not in col_dict.keys():
        col_dict[tn] = []
    colset.add(cn)
    col_dict[tn].append(cn)

for tn, cn, in zip(log_columns['table_name'],log_columns['column_name']):
    if cn in colset:
        # print('already have col')
        continue
    if tn not in col_dict.keys():
        col_dict[tn] = []
    colset.add(cn)
    col_dict[tn].append(cn)

# print(col_dict)


# %%
for i in col_dict.keys():
    print(i)
    print(col_dict[i])



# %%
news = set()
news.add('log_table')
print(set(col_dict.keys()) - news)
spec_col_set = (set(col_dict.keys()) - news)


# %%
col_sql = ""
for j in col_dict['log_table']:
    col_sql+=f"log_table.{j}\n,"
print(col_sql)

# %%

for i in spec_col_set:
    for j in col_dict[i]:
        col_sql+=f"{i}.{j}\n,"
col_sql = col_sql[:-2]
print(col_sql)



# %%
join_sql = "FROM log_table"
first = True
last_table = 'log_table'
for i in spec_col_set:
    if first:
        join_sql+=f"\nleft join {i} on {last_table}.GAME_ID = {i}.GAME_ID and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
        first = False
        last_table = i
    else:
        join_sql+=f"\nleft join {i} on {last_table}.GAME_ID = {i}.GAME_ID and {last_table}.PLAYER_NAME = {i}.PLAYER_NAME"
        last_table=i


print(join_sql)


# %%
combined_f_string =f"""CREATE OR REPLACE TABLE players_combined as
    SELECT 
    {col_sql}
{join_sql}"""

print(combined_f_string)


# %%
conn.execute(combined_f_string).df()




### LATEST: need to map lowercase cols to uppercase cols for traditional, or go through endpoints and update everything to v3






# %%

# %%
conn.execute(f"""
    COPY 
             (SELECT table_name, column_name 
             FROM information_schema.columns 
             WHERE table_name ilike '%players%'
             )
    to 'colstemp.csv' (HEADER, DELIMITER ',')
             """).df()

# %%
conn.execute("""SELECT * 
            FROM players_advanced 
            limit 100""").df()





# %%
### COMBINE TEAM DATA
conn.execute(f"""
    CREATE OR REPLACE TABLE teams_combined2 as
    SELECT *
    FROM log_table lt
    LEFT JOIN teams_advanced ta on lt.GAME_ID = ta.GAME_ID and lt.TEAM_ID = ta.TEAM_ID
    LEFT JOIN teams_fourfactors tff on ta.GAME_ID = tff.GAME_ID and ta.TEAM_ID = tff.TEAM_ID
    LEFT JOIN teams_misc tm on ta.GAME_ID = tm.GAME_ID and ta.TEAM_ID = tm.TEAM_ID
    LEFT JOIN teams_scoring ts on ta.GAME_ID = ts.GAME_ID and ta.TEAM_ID = ts.TEAM_ID
    LEFT JOIN lines_table linet on ta.GAME_ID = linet.GAME_ID and ta.TEAM_ABBREVIATION = linet.TEAM_ABBREVIATION
""").df()

# %%
### COMBINE TEAM DATA
conn.execute(f"""
    CREATE OR REPLACE TABLE teams_combined as
    SELECT *
    FROM log_table lt
    INNER JOIN teams_advanced ta on lt.GAME_ID = ta.GAME_ID and lt.TEAM_ID = ta.TEAM_ID
    INNER JOIN teams_fourfactors tff on ta.GAME_ID = tff.GAME_ID and ta.TEAM_ID = tff.TEAM_ID
    INNER JOIN teams_misc tm on ta.GAME_ID = tm.GAME_ID and ta.TEAM_ID = tm.TEAM_ID
    INNER JOIN teams_scoring ts on ta.GAME_ID = ts.GAME_ID and ta.TEAM_ID = ts.TEAM_ID
    INNER JOIN lines_table linet on ta.GAME_ID = linet.GAME_ID and ta.TEAM_ABBREVIATION = linet.TEAM_ABBREVIATION
""").df()

# %%
### COMBINE PLAYER DATA
conn.execute(f"""
    CREATE OR REPLACE TABLE players_combined as
    SELECT *
    FROM log_table lt
    inner join players_advanced ta on ta.GAME_ID = lt.GAME_ID and ta.TEAM_ABBREVIATION = lt.TEAM_ABBREVIATION
    LEFT JOIN players_fourfactors tff on ta.GAME_ID = tff.GAME_ID and ta.PLAYER_NAME = tff.PLAYER_NAME
    LEFT JOIN players_misc tm on ta.GAME_ID = tm.GAME_ID and ta.PLAYER_NAME = tm.PLAYER_NAME
    LEFT JOIN players_scoring ts on ta.GAME_ID = ts.GAME_ID and ta.PLAYER_NAME = ts.PLAYER_NAME
""").df()



# %%

conn.execute(f"""
    SELECT 
    * 
             from 
             players_combined
             limit 100
             """).df()

# %%
conn.execute(f"""
    SELECT 
             distinct season_id, count(*)

    FROM players_combined
             
    group by season_id
    order by season_id
             """).df()

# %%
conn.execute(f"""
    SELECT 
    * 
             from 
             players_combined
             where PLAYER_NAME = 'Damian Lillard'
             and season_id = 22023
             limit 100
             """).df()


# %%
conn.execute(f"""
    COPY 
             (SELECT 
    * 
             from 
             players_combined
             WHERE season_id = 22023
             order by GAME_DATE
             )
    to 'players_combined_2023-24.csv' (HEADER, DELIMITER ',')
             """).df()

# %%

conn.execute(f"""
    SELECT 
             distinct season_id, count(*)

    FROM TEAMS_COMBINED
             
    group by season_id
    order by season_id
             """).df()

# %%

conn.execute(f"""
    SELECT 
             distinct season_id, count(*)

    FROM TEAMS_COMBINED2
             
    group by season_id
    order by season_id
             """).df()



# %%

new = conn.execute(f"""
    SELECT *

    FROM TEAMS_COMBINED2
             
    WHERE season_ID = 42022
    LIMIT 10
             """).df()

new.to_csv('ex1.csv')


# %%

# new = 
conn.execute(f"""
    SELECT *

    FROM TEAMS_COMBINED
             
    WHERE season_ID = 42019
    LIMIT 10
             """).df()


new.to_csv('ex1.csv')


### explore
# %%

conn.execute(f"""
    SELECT count(*)

    FROM players_fourfactors
             
    LIMIT 10
             """).df()




# %%

conn.execute(f"""
    SELECT count(*)

    FROM players_fourfactors
             
    LIMIT 10
             """).df()

# %%
conn.execute(f"""
    SELECT count(*)

    FROM players_combined
             
    LIMIT 10
             """).df()


# %%

conn.execute(f"""
    SELECT count(*)

    FROM teams_fourfactors
             
    LIMIT 10
             """).df()

# %%
conn.execute(f"""
    SELECT count(*)

    FROM teams_combined
             
    LIMIT 10
             """).df()













# %%
conn.execute(f"""select *
             
             from players_advanced
             
             limit 10""").fetchall()

# %%
conn.execute(f"""select POSS
             
             from teams_advanced
             
             limit 10""").fetchall()
# %%
colset =[]
for kind in current_tables:
    sv_files = glob.glob(f'DATA/raw/teams/{kind}/*.csv')
    newdf = pd.read_csv(sv_files[0])
    print(len(newdf.columns))
    colset.append(set(newdf.columns))


# %%
print(colset[0].intersection(colset[1].intersection(colset[2].intersection(colset[3]))))

# %%
colset =[]
for kind in current_tables:
    sv_files = glob.glob(f'DATA/raw/players/{kind}/*.csv')
    newdf = pd.read_csv(sv_files[0])
    print(len(newdf.columns))
    colset.append(set(newdf.columns))


# %%
print(colset[0].intersection(colset[1].intersection(colset[2].intersection(colset[3]))))
# %%
columns = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'teams_combined'").fetchall()


print(type(columns))
print(columns)
# %%


conn.execute("SELECT * FROM teams_scoring where GAME_ID = '0021201227'").fetchdf()



# %%
# log_csv_files = glob.glob(f'DATA/raw/log/*.csv')
# for i in log_csv_files:
#     newdf = pd.read_csv(i)
#     print(i)
#     print(len(newdf.columns))


# %%

# newdf = pd.read_csv('DATA/raw/log/log_2021-22.csv')
# # newdf.drop_index()
# print(newdf.columns)
# newdf.drop(newdf.columns[[0]], axis=1, inplace = True)
# print(newdf.columns)
# newdf.to_csv('DATA/raw/log/log_2021-22.csv', index=False)



