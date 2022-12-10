import pandas as pd
import os
from tqdm import tqdm
import subprocess


def execute_bash(bashCommand):
    
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()


def download_train_data(fpath = 'features_metadata_FzP19JI.csv',wpath = 'data/sentinel/'):
    features = pd.read_csv(fpath)
    for k,row in tqdm(features.iterrows()):
        bashCommand = """aws s3 cp 
        {} {} --no-sign-request""".format(row['s3path_us'],wpath)

        execute_bash(bashCommand)




def download_agbm_data(fpath = 'train_agbm_metadata.csv',wpath = 'data/target/'):
    features = pd.read_csv(fpath)
    for k,row in tqdm(features.iterrows()):
        bashCommand = """aws s3 cp 
        {} {} --no-sign-request""".format(row['s3path_us'],wpath)

        execute_bash(bashCommand)



def main():
    download_train_data()
    download_agbm_data()

if __name__=='__main__':
    main()
