# %% 
import glob
import time
from os import path
import pandas as pd


# %%
# Define the directory where your CSV files are located
directory = "DATA/*/*/*"

# Use glob to find all CSV files in the directory
csv_files = glob.glob(path.join(directory, "*.csv"))

# Print the list of CSV files found
for csv_file in csv_files:
    print(csv_file)


# %%
for file in csv_files:
    new_df = pd.read_csv(file)
    # print(new_df.columns)
    if 'Unnamed: 0' in new_df.columns:
        print(f"fixing {file}")
    else:
        print(f"skipping {file}")
        continue
    # print(new_df)
    new_df = new_df.drop(columns = ['Unnamed: 0'])
    # newname = file[:-4] + '_fixed.csv'
    # print(newname)
    # new_df.to_csv(newname, index=False)
    new_df.to_csv(file, index=False)

    # break
# %%
