from data_scrapper.twitter import scrapper
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
        tweets_df = scrapper.get_tweets(common.KEYWORDS, common.USERNAMES, fromDate, toDate,'', 100)
        print(tweets_df)
        processor.process_data(tweets_df)
        