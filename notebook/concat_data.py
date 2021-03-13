# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
import glob
import re
import numpy as np

pd.options.display.max_columns=None
pd.options.display.max_rows=None
# -

horse_csvs_path = "/Users/daikimiyazaki/workspace/pndnism/horse_race_prediction/horse_info_crawler/horse_info_crawler/pedigree/data/horse_info/**/*.csv"
race_csvs_path = "/Users/daikimiyazaki/workspace/pndnism/horse_race_prediction/horse_info_crawler/horse_info_crawler/race/data/race_histories/**/*.csv"

horse_csvs = glob.glob(horse_csvs_path,recursive=True)
race_csvs = glob.glob(race_csvs_path,recursive=True)

concat_list = []
for horse_csv in horse_csvs:
    concat_list.append(pd.read_csv(horse_csv, dtype="object"))
horse_data = pd.concat(concat_list)
horse_data.drop_duplicates(inplace=True)

concat_list = []
for race_csv in race_csvs:
    concat_list.append(pd.read_csv(race_csv, dtype="object"))
race_data = pd.concat(concat_list)
race_data.drop_duplicates(inplace=True)

test = "https://db.netkeiba.com/race/202004040406/"
re.findall('/(\w+)/$',test)


def extract_id_number(x):
    if not re.findall("/(\w+)/$", x):
        return np.nan
    return re.findall("/(\w+)/$", x)[0]


race_data["race_id"] = race_data["race_url"].apply(extract_id_number)
horse_data["horse_id"] = horse_data["horse_url"].apply(extract_id_number)

race_data.to_csv("../data/race_stacking_data.csv", index=False)
horse_data.to_csv("../data/horse_stacking_data.csv", index=False)

horse_data.head()

list(horse_data.columns)

race_data.columns


