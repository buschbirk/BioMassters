import pandas as pd
import os
from tqdm import tqdm
import subprocess


def execute_bash(bashCommand):
    
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()


def download_train_data(fpath = 'features_metadata_FzP19JI.csv',wpath = 'data/sentinel/',num_datapoints=5):
    features = pd.read_csv(fpath)
    cur_chipid = None
    cc=0
    for k,row in tqdm(features.iterrows()):
        

        bashCommand = """aws s3 cp 
        {} {} --no-sign-request""".format(row['s3path_us'],wpath)

        execute_bash(bashCommand)
        if cur_chipid!= row['chip_id']:
            cur_chipid =  row['chip_id']
            cc+=1
            if cc >= num_datapoints:
                break
            pd.DataFrame({'chip_id':[row['chip_id']]}).to_csv('downloaded_chips.csv',mode='a')





def download_agbm_data(fpath = 'train_agbm_metadata.csv',wpath = 'data/target/'):
    features = pd.read_csv(fpath)
    downloaded_chips = pd.read_csv('downloaded_chips.csv')
    features = features.loc[features.chip_id.isin(downloaded_chips.chip_id.values.tolist())]
    for k,row in tqdm(features.iterrows()):
        bashCommand = """aws s3 cp 
        {} {} --no-sign-request""".format(row['s3path_us'],wpath)

        execute_bash(bashCommand)



def main():
    download_train_data()
    download_agbm_data()

if __name__=='__main__':
    main()
