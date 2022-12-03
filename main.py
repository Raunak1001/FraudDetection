from data_scrapper.twitter import scrapper
from extractor.twitter import data_converter
from processor.twitter import processor
import common

if __name__ == "__main__":
    print("____menu_____")
    print("1: to find user generated fraudulent data")
    print("2: to find fraudulent data for specific identifier")
    
    ch = int(input())
    
    if ch == 1:
        fromDate = input("please enter from date in YYYY-MM-DD format : ")
        toDate = input("please enter to date in YYYY-MM-DD format : ")
        tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, mentions=common.MENTIONS, from_time=fromDate, to_time=toDate, n=1000)
        print(tweets_df)
        extracted_df = data_converter.get_fraud_data(tweets_df)
        print(extracted_df)
        final_df = processor.process_data(extracted_df)
        print(final_df)
