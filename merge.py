import pandas as pd
import os
import glob
import common

path = os.getcwd()
print(path)
csv_files = glob.glob(os.path.join(path+"/", "*1.csv"))
uniq_vals = {''}
final_result = []

def merge():
    print(csv_files)
    for f in csv_files:
        df = pd.read_csv(f)
        for ind, row in df.iterrows():
            if row[common.IDENTIFIER_VALUE] in uniq_vals:
                continue

            uniq_vals.add(row[common.IDENTIFIER_VALUE])
            final_result.append([row[common.IDENTIFIER_TYPE], row[common.IDENTIFIER_VALUE], row[common.SOURCE], row[common.SCORE], row[common.ACCURACY], 
                            row[common.VERIFIED_SCORE], row[common.FOLLOWERS_SCORE], row[common.FINAL_SCORE], row[common.TWEET_URL], row[common.CONTENT]])

    final_df =  pd.DataFrame(final_result,
                        columns=[common.IDENTIFIER_TYPE, common.IDENTIFIER_VALUE, common.SOURCE, common.SCORE, common.ACCURACY, 
                        common.VERIFIED_SCORE, common.FOLLOWERS_SCORE, common.FINAL_SCORE, common.TWEET_URL, common.CONTENT])
    final_df.to_csv('merge.csv')

merge()