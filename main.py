from data_scrapper.twitter import scrapper
from extractor.twitter import data_converter
from processor.twitter import processor
import common
import pandas as pd
import warnings
import datetime
from multiprocessing import Process
import time

warnings.simplefilter(action='ignore', category=FutureWarning)
TEST_ENV = True


def detect_fraud(count, fromDate, toDate, n):
    tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=['number', 'mobile', 'phone', 'no', 'ph'],
                                    from_time=fromDate, to_time=toDate, n=n,
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
    final_df.to_csv('file-' + str(count) + '.csv')
    print("****** Final output ready ******")


if __name__ == "__main__":
    print("____menu_____")
    print("1: Number of tweets analysis")
    print("2: Date Range Analysis")
    print("3: Targeted Fraud Detection")
    ch = int(input())
    if ch==1:
        n = input("Enter the number of tweets: ")
        n = int(int(n)/13)
        detect_fraud(0, None, None, int(n))
    if ch == 2:
        fromDate = input("Enter from date: ")
        toDate = input("Enter to date: ")
        from_time = datetime.datetime.strptime(fromDate, '%Y-%m-%d').date()
        to_time = datetime.datetime.strptime(toDate, '%Y-%m-%d').date()
        new_to_time = from_time
        count = 0
        i = 0
        threads = []
        while new_to_time < to_time:
            i += 1
            count = count + 1
            new_to_time = from_time + datetime.timedelta(days=2)
            threads.append(Process(target=detect_fraud,
                                   args=(i, from_time.strftime('%Y-%m-%d'), new_to_time.strftime('%Y-%m-%d'), common.TWEET_COUNT,)))
            if new_to_time > to_time:
                new_to_time = to_time
            if count % 15 == 0:
                count = 0
                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()
                time.sleep(0.25)
                threads.clear()
            from_time = new_to_time

        count = 0
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        time.sleep(0.25)

    if ch == 3:
        phone_number = input("Enter the identifier for detection:")
        result_data = [[common.PHONE_NUMBER, phone_number, common.TWITTER, 0, '', 0, 0, 0, 0, '']]
        final_df = processor.process_data(pd.DataFrame(result_data,
                                                       columns=[common.IDENTIFIER_TYPE, common.IDENTIFIER_VALUE,
                                                                common.SOURCE, common.SCORE, common.CONTENT,
                                                                common.VERIFIED_SCORE, common.FOLLOWERS_SCORE,
                                                                common.FINAL_SCORE, common.ACCURACY, common.TWEET_URL]))
        print(final_df[common.ACCURACY])
