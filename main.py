from data_scrapper.twitter import scrapper
from extractor.twitter import data_converter
from processor.twitter import processor
import common

TEST_ENV = True

if __name__ == "__main__":
    print("____menu_____")
    print("1: to find user generated fraudulent data")
    print("2: to find fraudulent data for specific identifier")
    
    ch = int(input())
    
    if ch == 1:
        fromDate = ''
        toDate = ''
        if TEST_ENV:
            fromDate = "2022-11-02"
            toDate = "2022-12-02"
        else:
            fromDate = input("please enter from date in YYYY-MM-DD format : ")
            toDate = input("please enter to date in YYYY-MM-DD format : ")
        tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=['number','mobile'],from_time=fromDate, to_time=toDate, n=100)
        print("********** Got tweets ********")
        extracted_df = data_converter.get_fraud_data(tweets_df)
        print(extracted_df)
        final_df = processor.process_data(extracted_df)
        print(final_df)