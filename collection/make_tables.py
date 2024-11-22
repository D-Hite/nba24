# %%
import pandas as pd
import glob
import time
import duckdb


class TableGenerator():

    def __init__(self):
        self.conn = duckdb.connect('firstdb.db')
        self.CURRENT_TABLES =  ['advanced', 'fourfactors', 'misc', 'scoring', 'traditional']
        self.player_combination_sql = ''
        self.team_combination_sql = ''

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

    def get_column_sources_most_populated(self,cols):
        """
        cols:pandas.DataFrame, EX:self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
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



    def analyze_columns(self, column, table_list):
        # for table in table_list:
        #     col_df = self.conn.execute(f"""SELECT {column}
        #                           FROM {table}""").df()
        #     print(col_df.describe())
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
            join_sql+=f"\nleft join {i} on {last_table}.GAME_ID = {i}.GAME_ID and {last_table}.TEAM_ABBREVIATION = {i}.TEAM_ABBREVIATION"
            last_table=i

        combined_f_string =f"""CREATE OR REPLACE TABLE teams_combined as
            SELECT 
            {col_sql}
        {join_sql}"""

        self.conn.execute(combined_f_string).df()
        

        self.sql_create_team_combination = combined_f_string

        return 'teams_combined table made'


    def create_team_and_player_tables(self):
        playerandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%players%'  or table_name = 'log_table') and table_name != 'players_combined'").df()
        teamandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%team%' or table_name = 'log_table') and table_name != 'teams_combined'").df()

        players_dict = self.get_column_sources_most_populated(playerandlog_columns)
        player_sql = self.sql_create_player_combination(players_dict)

        team_dict = self.get_column_sources_most_populated(teamandlog_columns)
        team_sql = self.sql_create_team_combination(team_dict)

        with open('temp/creationsql.sql','w') as f1:
            f1.write(f"TEAMS:\n{self.sql_create_team_combination}\n\n")
            f1.write(f"PLAYERS:\n{self.sql_create_player_combination}\n\n")
        print("create_team_and_player_tables: DONE")




# %%
### REMAKING ENTIRE DATABASE
tg = TableGenerator()
tg.recreate_raw_tables()
tg.create_team_and_player_tables()


# %%
tg.conn.close()
