# %%
import pandas as pd
import glob
import time
import duckdb
from os import path



# %%
# conn = duckdb.connect('../collection/firstdb.db')
# %%
basepath = path.dirname(__file__)
filepath = path.abspath(path.join(basepath, "..", "collection", "firstdb.db"))

print(filepath)


# %%
conn = duckdb.connect(filepath)

# %%
conn.execute('SHOW TABLES').df()

# %%
"""
want to have a dynamic sql quiery that can grab data given the parameters:
columns
team or player data
seasons
offseason or normal
average data, rolling average data or static data
next game outcome data
line data
"""

# %%

conn.close()
# %%
