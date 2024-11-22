# %%
import pandas as pd
import glob
import time
import duckdb



conn = duckdb.connect('firstdb.db')
CURRENT_TABLES = ['advanced', 'fourfactors', 'misc', 'scoring', 'traditional']


def create_log_table():
    ## log data
    log_data_path ='DATA/raw/log/'
    log_csv_files = glob.glob(f'{log_data_path}*.csv')
    if not log_csv_files:
        print(f'create_table_log error: NO FILES in {log_data_path}')
        return

    try:
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
    except Exception as e:
        print(f"create_table_log error: {e}")
        return
    
    print('SUCCESSFULLY MADE LOG TABLE')
    return
        
    
def create_line_table():
    ## line data
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
        return
    
    print('SUCCESSFULLY MADE LINE TABLE')

def create_stat_tables():
    ## game data
    for tp in ['teams', 'players']:
        for kind in CURRENT_TABLES:
            try:
                current_path = f'DATA/raw/{tp}/{kind}/'
                csv_files = glob.glob(f'{current_path}*.csv')
                if not csv_files:
                    print(f"create_stat_tables error for {tp}, {kind}. no files in {current_path}")
                    continue

                conn.execute(f"""
                    CREATE OR REPLACE TABLE {tp}_{kind} as
                    SELECT *
                    FROM read_csv_auto('{csv_files[0]}'
                    )
                """).df()


                for file in csv_files[1:]:
                    conn.execute(f"""
                        INSERT INTO {tp}_{kind}
                        SELECT *
                        FROM read_csv_auto('{file}'
                        )
                        """)
                
                print(f'SUCCESSFULLY CREATED TABLE {tp}_{kind}')
                
            except Exception as e:
                print(f"problem with {tp} {kind}\n{e}")
                return
            
    return
            


def recreate_raw_tables():
    create_log_table()
    create_line_table()
    create_stat_tables()



# %%
# ### Create tables    


recreate_raw_tables()




### todo: combine tables, processed data, dynamic query for model
# %%
conn.execute(f"""SHOW TABLES""").fetchall()

# %%
conn.execute("SELECT * FROM information_schema.columns WHERE table_name ilike '%teams_advanced%'").df()

# %%
player_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%'").df()
log_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 'log_table'").df()
playerandlog_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()


# %%
print(playerandlog_columns)



# %%

# # original
# def get_column_sources(cols, logcols):
#     """
#     cols:pandas.DataFrame, EX:conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%'").df()
#     logcols = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 'log_table'").df()
#     """
#     colset = set()
#     col_dict = dict()
#     for tn, cn, in zip(cols['table_name'],cols['column_name']):
#         if cn in colset:
#             # print('already have col')
#             continue
#         if tn not in col_dict.keys():
#             col_dict[tn] = []
#         colset.add(cn)
#         col_dict[tn].append(cn)

#     for tn, cn, in zip(logcols['table_name'],logcols['column_name']):
#         if cn in colset:
#             # print('already have col')
#             continue
#         if tn not in col_dict.keys():
#             col_dict[tn] = []
#         colset.add(cn)
#         col_dict[tn].append(cn)


def get_column_sources_most_populated(cols):
    """
    cols:pandas.DataFrame, EX:conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
    """
    col_dict = dict()
    out_dict = dict()
    ## FIRST GET ALL COLUMNS IN DICTIONARY
    for tn, cn, in zip(cols['table_name'],cols['column_name']):
        if tn not in out_dict.keys():
            out_dict[tn] = []
        if cn not in col_dict.keys():
            col_dict[cn] = []
        col_dict[cn].append(tn)
    
    for cn2 in col_dict.keys():
        if len(col_dict[cn2]) > 1:
            newlist = set(col_dict[cn2]) - set(['log_table']) # need to prioritizeother tables over log table (for player data)
            best_table = analyze_columns(cn2, list(newlist))
            out_dict[best_table].append(cn2)
        elif len(col_dict[cn2]) == 0:
            print('DATA ERROR? in get_column_sources_most_populated')
        else:
            out_dict[col_dict[cn2][0]].append(cn2)
    
    return out_dict



def analyze_columns(column, table_list):
    # for table in table_list:
    #     col_df = conn.execute(f"""SELECT {column}
    #                           FROM {table}""").df()
    #     print(col_df.describe())
    return table_list[0]

col_dict = get_column_sources_most_populated(playerandlog_columns)



# %%
def sql_create_player_combination(col_dict):

    news = set()
    news.add('log_table')
    # print(set(col_dict.keys()) - news)
    spec_col_set = (set(col_dict.keys()) - news)

    col_sql = ""
    for j in col_dict['log_table']:
        col_sql+=f"log_table.{j}\n,"
    # print(col_sql)

    for i in spec_col_set:
        for j in col_dict[i]:
            col_sql+=f"{i}.{j}\n,"
    col_sql = col_sql[:-2]
    # print(col_sql)

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

    combined_f_string =f"""CREATE OR REPLACE TABLE players_combined as
        SELECT 
        {col_sql}
    {join_sql}"""

    conn.execute(combined_f_string).df()

    return combined_f_string



def sql_create_team_combination(col_dict):

    news = set()
    news.add('log_table')
    # print(set(col_dict.keys()) - news)
    spec_col_set = (set(col_dict.keys()) - news)

    col_sql = ""
    for j in col_dict['log_table']:
        col_sql+=f"log_table.{j}\n,"
    # print(col_sql)

    for i in spec_col_set:
        for j in col_dict[i]:
            col_sql+=f"{i}.{j}\n,"
    col_sql = col_sql[:-2]
    # print(col_sql)

    join_sql = "FROM log_table"
    first = True
    last_table = 'log_table'
    for i in spec_col_set:
        join_sql+=f"\nleft join {i} on {last_table}.GAME_ID = {i}.GAME_ID and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
        last_table=i

    combined_f_string =f"""CREATE OR REPLACE TABLE teams_combined as
        SELECT 
        {col_sql}
    {join_sql}"""

    conn.execute(combined_f_string).df()

    return combined_f_string



# %%
playerandlog_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
teamandlog_columns = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%team%' or table_name = 'log_table'").df()

players_dict = get_column_sources_most_populated(playerandlog_columns)
player_sql = sql_create_player_combination(players_dict)

team_dict = get_column_sources_most_populated(teamandlog_columns)
team_sql = sql_create_team_combination(team_dict)

with open('temp/creationsql.sql','w') as f1:
    f1.write(f"TEAMS:\n{team_sql}\n\n")
    f1.write(f"PLAYERS:\n{player_sql}\n\n")



# %%
newest = conn.execute("SELECT * from teams_combined order by RANDOM() limit 1000").df()
newest.to_csv('temp/random1000.csv')


# %%
conn.execute("SELECT count(*) as empty from teams_combined where PLUS_MINUS is null").df()
conn.execute("SELECT count(*) from teams_combined").df()


# newest.to_csv('temp/random1000.csv')






# # %%
# ### SAMPLE ANALYZE COLUMNS

# column = 'OREB_PCT'
# table_list = ['players_advanced', 'players_fourfactors']

# for table in table_list:

#     # col_df = conn.execute(f"""SELECT {column}, count({column})
#     #                         FROM {table}
#     #                         group by {column}
#     #                         order by count({column})""").df()
#     # col_df = conn.execute(f"""SELECT count({column})
#     #                          FROM {table}
#     #                          where {column} is NULL""").df()
#     col_df_normal = conn.execute(f"""SELECT GAME_ID, PLAYER_ID, {column}
#                             FROM {table}
#                             ORDER BY GAME_ID, PLAYER_ID""").df()
#     col_df_normal.to_csv(f'temp/{table}_{column}.csv')
#     # print(col_df)
#     # print(f"{table} NA: {col_df_normal[column].isna().sum()}")


### LATEST: need to map lowercase cols to uppercase cols for traditional, or go through endpoints and update everything to v3


# %%
conn.close()

# %%
