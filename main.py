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
    print("____menu_____")
    print("1: to find user generated fraudulent data")
    print("2: to find fraudulent data for specific identifier")


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
        tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=['number', 'mobile', 'phone', 'no', 'ph'],
                                        from_time=fromDate, to_time=toDate, n=common.TWEET_COUNT,
                                        operator=common.OPERATOR_AND, mentions=common.MENTIONS)
        print("********** Got tweets ********")
        tweets_df = scrapper.fill_media_content(tweets_df)
        tweets_df.to_csv('raw_data.csv')
        print("******** Extracted media ******")
        extracted_df = data_converter.get_fraud_data(tweets_df)
        extracted_df.to_csv('extracted_data.csv')
        print("******** Extracted data ********")
        final_df = processor.process_data(extracted_df)
        final_df = final_df.sort_values(by=[common.FINAL_SCORE], ascending=False)
        final_df.to_csv('file1.csv')
        print("****** Final output ready ******")
    if ch == 2:
        phone_number = input("please enter the phone number to check: ")
        result_data = [[common.PHONE_NUMBER, phone_number, common.TWITTER, 0, '', 0, 0, 0]]
        final_df = processor.process_data(pd.DataFrame(result_data,
                                                       columns=[common.IDENTIFIER_TYPE, common.IDENTIFIER_VALUE,
                                                                common.SOURCE, common.SCORE, common.CONTENT,
                                                                common.VERIFIED_SCORE, common.FOLLOWERS_SCORE,
                                                                common.FINAL_SCORE]))
        print(final_df[common.SCORE], final_df[common.VERIFIED_SCORE], final_df[common.FOLLOWERS_SCORE],
              final_df[common.FINAL_SCORE])
