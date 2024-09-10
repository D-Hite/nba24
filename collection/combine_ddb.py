# %%
import pandas as pd
import glob
import time
import duckdb

conn = duckdb.connect() # create an in-memory database
# %%
# with duckdb
cur_time = time.time()
df = conn.execute("""
	SELECT *
	FROM 'data2/*.csv'
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
# %%

df = conn.execute("""
	SELECT *
	FROM 'data2/*.csv'
""").df()
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df() # doesn't work if you don't register df as a virtual table
# %%


cur_time = time.time()
df = conn.execute("""
	SELECT 
                  "season_id",
                  "GAME_DATE",
                  "MATCHUP",
                  "TEAM_NAME",
                  "PTS"
	FROM 'data2/*.csv'
    order by "PTS" desc
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
# %%