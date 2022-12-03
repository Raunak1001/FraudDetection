from data_scrapper.twitter import scrapper
from extractor.twitter import data_converter
from processor.twitter import processor
import common
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
        ch = int(input())
    if ch == 1:
        tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=['number','mobile'],from_time=fromDate, to_time=toDate, n=100, operator=common.OPERATOR_AND)
        print("********** Got tweets ********")
        extracted_df = data_converter.get_fraud_data(tweets_df)
        print("**** phone numbers ***** {0}".format(extracted_df))
        final_df = processor.process_data(extracted_df)
        print("**** final result ***** {0}".format(extracted_df))
