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

    def get_column_sources_most_populated(self,cols,player_data):
        """
        used to select which columns come from what source

        cols:pandas.DataFrame, EX:self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
        player_data: True if player data
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
            if not player_data:## TEAM_DATA, prioritize log data
                if 'log_table' in col_dict[cn2]:
                    out_dict['log_table'].append(cn2)
                    continue
            elif cn2 in ['SEASON_ID','TEAM_ID','TEAM_ABBREVIATION','TEAM_NAME','GAME_ID','GAME_DATE','MATCHUP','WL']:## PLAYER DATA LOG DATA
                out_dict['log_table'].append(cn2)
                continue
            if len(col_dict[cn2]) > 1:
                newlist = set(col_dict[cn2]) - set(['log_table']) # need to prioritizeother tables over log table (for player data)
                best_table = self.analyze_columns(cn2, list(newlist))
                out_dict[best_table].append(cn2)
            elif len(col_dict[cn2]) == 0:
                print('DATA ERROR? in get_column_sources_most_populated')
            else:
                out_dict[col_dict[cn2][0]].append(cn2)
        
        return out_dict



    def analyze_columns(self, column, table_list):
        for table in table_list:
            if 'fourfactors' in table.lower():
                return table
        return table_list[0]


    def sql_create_player_combination(self,col_dict):

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

        self.conn.execute(combined_f_string).df()

        self.sql_create_player_combination = combined_f_string

        return 'players_combined table made'



    def sql_create_team_combination(self,col_dict):

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
            join_sql+=f"\nleft join {i} on {last_table}.GAME_ID::int = {i}.GAME_ID::int and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
            # last_table=i

        combined_f_string =f"""CREATE OR REPLACE TABLE teams_combined as
            SELECT DISTINCT
            {col_sql}
        {join_sql}"""

        self.conn.execute(combined_f_string).df()
        

        self.sql_create_team_combination = combined_f_string

        return 'teams_combined table made'


    def create_team_and_player_tables(self):
        playerandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%players%'  or table_name in ('log_table')) and table_name != 'players_combined'").df()
        teamandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%teams%' or table_name in ('log_table', 'lines_table')) and table_name != 'teams_combined'").df()

        players_dict = self.get_column_sources_most_populated(playerandlog_columns, True)
        player_sql = self.sql_create_player_combination(players_dict)

        team_dict = self.get_column_sources_most_populated(teamandlog_columns,False)
        team_sql = self.sql_create_team_combination(team_dict)

        with open('creationsql.sql','w') as f1:
            f1.write(f"TEAMS:\n{self.sql_create_team_combination}\n\n")
            f1.write(f"PLAYERS:\n{self.sql_create_player_combination}\n\n")
        print("create_team_and_player_tables: DONE")




# %%
### REMAKING ENTIRE DATABASE
tg = TableGenerator()
tg.recreate_raw_tables()
tg.create_team_and_player_tables()





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
