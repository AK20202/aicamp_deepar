import pandas as pd
import numpy as np
import gc
import os

###  Settings

with open('settings.json', 'r') as j:
    json_data = json.load(j)

project_path = json_data["project_path"]

data_path = os.path.join(project_path, "data")
python_path = os.path.join(project_path, "python")

raw_data_path = os.path.join(data_path, "raw", "with_loc")
output_data_path = os.path.join(data_path, "output")
supporting_dp = os.path.join(data_path, "supporting")
data_chunks = os.path.join(data_path,"data_chunks")

import sys
sys.path.append(python_path)

import utils

fn = os.path.join(supporting_dp, "target_region_id_list.csv")
target_region_id_list = pd.read_csv(fn, header=0, sep=",")["target_region_id_list"].to_list()

#%%time
tmp = []

sbf = str(sys.argv[1])
#print(sbf)

file_list = []
for (dirpath, dirnames, filenames) in os.walk(os.path.join(raw_data_path, sbf)):
    file_list.extend(filenames)
for f in file_list:
    print (f)
    f_path = os.path.join(raw_data_path, sbf, f)
    dt = pd.read_csv(f_path, sep = ',', header = 0)
    dt = utils.target_column_clean_up(dt)
    dt_cl = utils.basic_data_cleaning_and_new_features_wk1(dt)
    counts_data = utils.get_counts_per_hour_per_region(dt_cl)
    tmp.append(counts_data.loc[counts_data.Square_id.isin(target_region_id_list)])
    del(dt)
    del(dt_cl)
    gc.collect()

dt = pd.concat(tmp, ignore_index = True)
dt.to_csv(os.path.join(data_chunks,'.'.join([sbf, 'csv'])))