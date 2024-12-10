# %%
import pandas as pd
import duckdb
from os import path



# %%
basepath = path.dirname(__file__)
filepath = path.abspath(path.join(basepath, "..", "collection", "firstdb.db"))

print(filepath)


# %%
conn = duckdb.connect(filepath)


# %%
conn.execute("SHOW TABLES").df()



# %%

conn.execute("""SELECT 
    lt.SEASON_ID,
    COUNT(DISTINCT tav.GAME_ID) AS GAME_IDs_in_TEAM_AVG_10_TABLE,
    COUNT(DISTINCT lt.GAME_ID) AS TOTAL_GAME_IDs_in_SEASON,
    (COUNT(DISTINCT tav.GAME_ID) * 1.0 / COUNT(DISTINCT lt.GAME_ID)) AS PERCENT_PRESENT
FROM 
    log_table lt
LEFT JOIN 
    TEAM_AVG_10_TABLE tav
    ON lt.GAME_ID = tav.GAME_ID
GROUP BY 
    lt.SEASON_ID
ORDER BY 
    lt.SEASON_ID;""").df()


# %%


conn.execute("""SELECT *
             from teams_combined where GAME_ID::int = 42100406""").df()








# %%
conn.close()


# %%
