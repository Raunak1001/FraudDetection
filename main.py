from data_scrapper.twitter import scrapper
from extractor.twitter import data_converter
from processor.twitter import processor
import common
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

fromDate = ''
toDate = ''
TEST_ENV = True

if __name__ == "__main__":
    if TEST_ENV:
        ch = 1
        # fromDate = '2022-11-02'
        # toDate = '2022-12-02'
    else:
        print("____menu_____")
        print("1: to find user generated fraudulent data")
        print("2: to find fraudulent data for specific identifier")
        ch = int(input())
    if ch == 1:
        tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=['number', 'mobile', 'phone', 'no', 'ph'],from_time=fromDate, to_time=toDate, n=common.TWEET_COUNT, operator=common.OPERATOR_AND, mentions=common.MENTIONS)
        print("********** Got tweets ********")
        # tweets_df = pd.concat([tweets_df1, tweets_df2])
        tweets_df1.to_csv('raw_data.csv')
        extracted_df = data_converter.get_fraud_data(tweets_df1)
        print("******** Extracted data ******\n {0}".format(extracted_df))
        final_df = processor.process_data(extracted_df)
        final_df = final_df.sort_values(by=[common.SCORE], ascending=False)
        final_df.to_csv('file1.csv')
