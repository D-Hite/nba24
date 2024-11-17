# %% 
import pandas as pd
import glob
import time
import duckdb
import nba_api.stats.endpoints as ep
# %%
new = pd.DataFrame()

print(new)

current_ep = 'traditional'
# %%
newnew = pd.read_csv(f"../DATA/raw/teams/{current_ep}/{current_ep}2023-24.csv")
# %%
print(newnew.columns)
# %%
# new_df = newnew.drop(columns = ['Unnamed: 0.1'])
# new_df.to_csv('../DATA/raw/teams/traditional/traditional2023-24.csv', index=False)

# %%
statfunc = ep.boxscoretraditionalv2.BoxScoreTraditionalV2
game = statfunc(game_id='0022300002').get_data_frames()
players=game[0]
teams=game[1]
# %%
print(players.columns)
# %%
print(teams.columns)
# %%
print(len(teams.columns))
# %%
print(len(newnew.columns))
# %%


### Traditional v2 v3 mapping
new_cols = {'gameId':'GAME_ID',
 'teamId':'TEAM_ID',
 'teamCity':'TEAM_CITY',
 'teamName':'TEAM_NAME',
 'teamTricode':'TEAM_ABBREVIATION',
 'teamSlug':'REMOVE',
 'minutes':'MIN',
 'fieldGoalsMade':'FGM',
 'fieldGoalsAttempted':'FGA',
 'fieldGoalsPercentage':'FG_PCT',
 'threePointersMade':'FG3M',
 'threePointersAttempted':'FG3A',
 'threePointersPercentage':'FG3_PCT',
 'freeThrowsMade':'FTM',
 'freeThrowsAttempted':'FTA',
 'freeThrowsPercentage':'FT_PCT',
 'reboundsOffensive':'OREB',
 'reboundsDefensive':'DREB',
 'reboundsTotal':'REB',
 'assists':'AST',
 'steals':'STL',
 'blocks':'BLK',
 'turnovers':'TO',
 'foulsPersonal':'PF',
 'points':'PTS',
 'startersBench':
}