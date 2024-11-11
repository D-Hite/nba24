# %% 
import pandas as pd
import glob
import time
import duckdb
import nba_api.stats.endpoints as ep
# %%
new = pd.DataFrame()

print(new)

# %%
newnew = pd.read_csv('../DATA/raw/teams/traditional/traditional2023-24.csv')
# %%
print(newnew.columns)
# %%
# new_df = newnew.drop(columns = ['Unnamed: 0.1'])
# new_df.to_csv('../DATA/raw/teams/traditional/traditional2023-24.csv', index=False)

# %%
statfunc = ep.boxscorefourfactorsv3.BoxScoreFourFactorsV3
game = statfunc(game_id='0022300002').get_data_frames()
players=game[0]
teams=game[1]
# %%
print(players)
# %%
print(teams)
# %%