# %%
import pandas as pd
import glob
import time
import duckdb


class TableGenerator():

    def __init__(self):
        self.conn = duckdb.connect('firstdb.db')
        self.CURRENT_TABLES =  self.get_endpoints()
        self.player_combination_sql = ''
        self.team_combination_sql = ''

    def get_endpoints(self):
        team_table_paths = glob.glob('DATA/raw/teams/*')
        team_tables = set([x.split('/')[-1] for x in team_table_paths])

        player_table_paths = glob.glob('DATA/raw/players/*')
        player_tables = set([x.split('/')[-1] for x in player_table_paths])

        return team_tables.intersection(player_tables)


    def create_log_table(self):
        ## log data
        log_data_path ='DATA/raw/log/'
        log_csv_files = glob.glob(f'{log_data_path}*.csv')
        if not log_csv_files:
            print(f'create_table_log error: NO FILES in {log_data_path}')
            return

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE log_table as
                SELECT *
                FROM read_csv_auto('{log_csv_files[0]}')
            """).df()

            for file in log_csv_files[1:]:
                self.conn.execute(f"""
                    INSERT INTO log_table
                    SELECT * FROM read_csv_auto('{file}')
                    """)
        except Exception as e:
            print(f"create_table_log error: {e}")
            return
        
        print('SUCCESSFULLY MADE LOG TABLE')
        return
            
        
    def create_line_table(self):
        ## line data
        line_data_path = 'DATA/lines/'
        lines_csv_files = glob.glob(f'{line_data_path}*.csv')
        if not lines_csv_files:
            print(f'create_line_table error: no file in {line_data_path}')


        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE lines_table as
                SELECT *
                FROM read_csv_auto('{lines_csv_files[0]}')
            """).df()

            for file in lines_csv_files[1:]:
                self.conn.execute(f"""
                    INSERT INTO lines_table
                    SELECT * FROM read_csv_auto('{file}')
                    """)
        except Exception as e:
            print(f"create_line_table error: {e}")
            return
        
        print('SUCCESSFULLY MADE LINE TABLE')

    def create_stat_tables(self):
        ## game data
        for tp in ['teams', 'players']:
            for kind in self.CURRENT_TABLES:
                try:
                    current_path = f'DATA/raw/{tp}/{kind}/'
                    csv_files = glob.glob(f'{current_path}*.csv')
                    if not csv_files:
                        print(f"create_stat_tables error for {tp}, {kind}. no files in {current_path}")
                        continue

                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {tp}_{kind} as
                        SELECT *
                        FROM read_csv_auto('{csv_files[0]}'
                        )
                    """).df()


                    for file in csv_files[1:]:
                        self.conn.execute(f"""
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
                


    def recreate_raw_tables(self):
        self.create_log_table()
        self.create_line_table()
        self.create_stat_tables()
        print('recreate_raw_tables: DONE')

    def get_column_sources(self,cols):
        """
        used to select which columns come from what source

        cols:pandas.DataFrame, EX:self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
        player_data: True if player data
        """
        col_dict = dict()
        ## FIRST GET ALL COLUMNS IN DICTIONARY
        for tn, cn, in zip(cols['table_name'],cols['column_name']):
            if cn not in col_dict.keys():
                col_dict[cn] = []
            col_dict[cn].append(tn)

        return col_dict

    def sql_create_player_combination(self,col_dict):
        """
        reads through col_dict generated from get_column_sources to make the players_combined 
        updated to coalesce values when there are multiple tables with the column
        
        """

        order_map = {'log_table':0,'players_fourfactors':1}

        spec_col_set = set()
        for value_list in col_dict.values():
            spec_col_set.update(value_list)
        spec_col_set.discard('log_table')

        col_sql = ""
        for col_name in col_dict.keys():
            if col_name == 'TOV':### THIS IS ONLY IN LOG TABLE and doesnt apply to player data
                continue
            # og_tables = col_dict[col_name]
            col_dict[col_name] = sorted(col_dict[col_name], key=lambda x: order_map.get(x, float('inf')))
            if col_name in ['SEASON_ID','TEAM_ID','TEAM_ABBREVIATION','TEAM_NAME','GAME_ID','GAME_DATE','MATCHUP','WL']:## PLAYER DATA LOG DATA
                col_dict[col_name] = ['log_table']
            elif 'log_table' in col_dict[col_name]:
                col_dict[col_name] = col_dict[col_name][1:]
            if len(col_dict[col_name]) == 1:
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: {col_dict[col_name][0]}.{col_name},")
                col_sql+=f"\n\t{col_dict[col_name][0]}.{col_name},"
            else:
                coalesce_statement = ""
                for table_name in col_dict[col_name]:
                    coalesce_statement+=f"{table_name}.{col_name},"
                coalesce_statement = coalesce_statement[:-1]
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: COALESCE({coalesce_statement}) as {col_name},")
                col_sql+=f"\n\tCOALESCE({coalesce_statement}) as {col_name},"

        join_sql = "FROM log_table"
        first = True
        last_table = 'log_table'
        for i in spec_col_set:
            if first:
                join_sql+=f"\nleft join {i} on {last_table}.GAME_ID::int = {i}.GAME_ID::int and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
                first = False
                last_table = i
            else:
                join_sql+=f"\nleft join {i} on {last_table}.GAME_ID::int = {i}.GAME_ID::int and {last_table}.PLAYER_NAME = {i}.PLAYER_NAME"
                last_table=i

        combined_f_string =f"""CREATE OR REPLACE TABLE players_combined as
            SELECT DISTINCT
            {col_sql}
        {join_sql}"""

        return combined_f_string



    def sql_create_team_combination(self,col_dict):

        order_map = {'log_table':0,'players_fourfactors':1}

        spec_col_set = set()
        for value_list in col_dict.values():
            spec_col_set.update(value_list)
        spec_col_set.discard('log_table')

        col_sql = ""
        for col_name in col_dict.keys():
            col_dict[col_name] = sorted(col_dict[col_name], key=lambda x: order_map.get(x, float('inf')))
            if len(col_dict[col_name]) == 1:
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: {col_dict[col_name][0]}.{col_name},")
                col_sql+=f"\n\t{col_dict[col_name][0]}.{col_name},"
            else:
                coalesce_statement = ""
                if col_name in ['GAME_ID','MIN']:### COLUMNS WITH DIFFERENT TYPES: TODO: MAKE THIS AUTOMATIC?
                    for table_name in col_dict[col_name]:
                        coalesce_statement+=f"CAST({table_name}.{col_name} AS VARCHAR),"
                else:
                    for table_name in col_dict[col_name]:
                        coalesce_statement+=f"{table_name}.{col_name},"
                coalesce_statement = coalesce_statement[:-1]
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: COALESCE({coalesce_statement}) as {col_name},")
                col_sql+=f"\n\tCOALESCE({coalesce_statement}) as {col_name},"

        join_sql = "FROM log_table"
        first = True
        last_table = 'log_table'
        for i in spec_col_set:
            join_sql+=f"\nleft join {i} on {last_table}.GAME_ID::int = {i}.GAME_ID::int and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
            # last_table=i

        combined_f_string =f"""CREATE OR REPLACE TABLE teams_combined as
            SELECT DISTINCT
            {col_sql}
        {join_sql}"""

        return combined_f_string


    def create_team_and_player_tables(self):
        playerandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%players%'  or table_name in ('log_table')) and table_name != 'players_combined'").df()
        teamandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%teams%' or table_name in ('log_table', 'lines_table')) and table_name != 'teams_combined'").df()

        players_dict = self.get_column_sources(playerandlog_columns)
        player_sql = self.sql_create_player_combination(players_dict)
        self.conn.execute(player_sql).df()
        self.sql_create_player_combination = player_sql

        team_dict = self.get_column_sources(teamandlog_columns)
        team_sql = self.sql_create_team_combination(team_dict)
        self.conn.execute(team_sql).df()
        self.sql_create_team_combination = team_sql

        with open('creationsql.sql','w') as f1:
            f1.write(f"TEAMS:\n{self.sql_create_team_combination}\n\n")
            f1.write(f"PLAYERS:\n{self.sql_create_player_combination}\n\n")
        print("create_team_and_player_tables: DONE")




# %%
### REMAKING ENTIRE DATABASE
tg = TableGenerator()
# tg.recreate_raw_tables()
tg.create_team_and_player_tables()


# %%
# conn = duckdb.connect('firstdb.db')
x = teamandlog_columns = conn.execute("""
with overalpping_cols as 
                                      (
SELECT column_name
FROM information_schema.columns
WHERE (table_name ILIKE '%teams%' OR table_name IN ('log_table', 'lines_table'))
  AND table_name != 'teams_combined'
GROUP BY column_name
HAVING COUNT(DISTINCT table_name) > 1
)
    SELECT table_name,
column_name,
data_type
FROM information_schema.columns
WHERE (table_name ILIKE '%teams%' OR table_name IN ('log_table', 'lines_table'))
  AND table_name != 'teams_combined'
                                      AND column_name in (select * from overalpping_cols)                      
                                      order by column_name
                                      ;
""").df()
x.to_csv('temp/column_teamssample.csv')


# %%

x = teamandlog_columns = conn.execute("""
WITH overlapping_cols AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE (table_name ILIKE '%teams%' OR table_name IN ('log_table', 'lines_table'))
      AND table_name != 'teams_combined'
    GROUP BY column_name
    HAVING COUNT(DISTINCT table_name) > 1
),
columns_with_diff_types AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE (table_name ILIKE '%teams%' OR table_name IN ('log_table', 'lines_table'))
      AND table_name != 'teams_combined'
    AND column_name IN (SELECT column_name FROM overlapping_cols)
    GROUP BY column_name
    HAVING COUNT(DISTINCT data_type) > 1
)
SELECT table_name,
       column_name,
       data_type
FROM information_schema.columns
WHERE (table_name ILIKE '%teams%' OR table_name IN ('log_table', 'lines_table'))
  AND table_name != 'teams_combined'
  AND column_name IN (SELECT column_name FROM columns_with_diff_types)
ORDER BY column_name, table_name;

""").df()
x.to_csv('temp/column_teamssample2.csv')

# %%
x = tg.conn.execute("select * from TEAMS_COMBINED WHERE GAME_ID = 21600597").df()
x.to_csv('temp/sample.csv')
# %%
print(tg.CURRENT_TABLES)

# %%
tg.conn.execute('show tables').df()

# %%
tg.conn.execute("""SELECT *
             from teams_combined where GAME_ID::int = 42100406""").df()

# %%
tg.conn.execute("""SELECT *
             from lines_table where GAME_ID::int = 42100406""").df()



# %%
### TEAM_SAMPLE
sample1 = tg.conn.execute('select * from teams_combined order by random() limit 1000').df()
sample1.to_csv('./temp/sample_teams_combined.csv')



# %%
### PLAYER_SAMPLE
sample2 = tg.conn.execute("""select * from players_combined where GAME_DATE::DATE > '2017-01-01'::DATE order by random() limit 1000""").df()
sample2.to_csv('./temp/sample_players_combined.csv')


# %%
### GAME_SAMPLE
sample3 = tg.conn.execute("""select * from players_combined where GAME_ID::int = 22000836""").df()
sample3.to_csv('./temp/one_game_players_combined.csv')
##22000836

# %%
tg.conn.close()

# %%
conn.close()
# %%
