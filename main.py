import matplotlib as plt
import os
import pandas as pd
import pathlib


directory = r'C:\Computer Learning\Kaggle Datasets\nba_datasets'
# reading csv datasets
'''
d1_loc = 'C:Computer Learning\Kaggle Datasets\basketball_datasetsgames.csv'
d1_loc.replace(" ", "_")
print(d1_loc)
'''

file_dict = {}
count = 0
# gets the count of files in directory
for path in pathlib.Path(directory).iterdir():
    if path.is_file():
        count += 1
        #print(count)

# Steps through the directory and gets the file names, dataset_path is the full path to each file
# file_dict contains the df from the files in the directory
    while count > 0:
        for file in os.listdir(directory):
            dataset_path = os.path.join(directory, file)
            file_dict[f'df{count}'] = pd.read_csv(dataset_path)
            count -= 1


print(file_dict[0])


