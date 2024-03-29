import threading
import time

import pandas as pd

from data_scrapper.twitter import scrapper
import common


def process_data(extracted_df):
    threads = []
    i = 0
    for ind, row in extracted_df.iterrows():
        if ind % 1000 == 0:
            print("Processing", ind, "th row")
        t = threading.Thread(target=process_parallel, args=(row, extracted_df, ind))
        threads.append(t)
        i = i + 1
        if i % common.CONCURRENCY_COUNT == 0:
            i = 0
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
            time.sleep(0.25)
            threads.clear()
    if i != 0:
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
    return extracted_df


def process_parallel(row, extracted_df, ind):
    if row[common.IDENTIFIER_TYPE] == common.PHONE_NUMBER:
        fraud_df = get_fraud_info(get_number_combinations(str(row[common.IDENTIFIER_VALUE])))
    else:
        fraud_df = get_fraud_info([row[common.IDENTIFIER_VALUE]])
    score, verified_count, followers_count, content, urls = get_score_and_content(fraud_df)
    extracted_df[common.SCORE][ind] = score
    extracted_df[common.VERIFIED_SCORE][ind] = verified_count
    extracted_df[common.FOLLOWERS_SCORE][ind] = followers_count
    extracted_df[common.CONTENT][ind] = content
    extracted_df[common.FINAL_SCORE][ind] = score + verified_count + followers_count
    extracted_df[common.ACCURACY][ind] = get_fraud_tier(score + verified_count + followers_count)
    extracted_df[common.TWEET_URL][ind] = urls


def get_fraud_info(identifier):
    tweets_df = pd.DataFrame()
    tweets_df = scrapper.scrape_tweets_sync(keywords1=common.KEYWORDS, keywords2=identifier, from_time='', to_time='',
                                            operator=common.OPERATOR_AND, n=100)
    return tweets_df


def get_number_combinations(phonenumber):
    combinations = [wrap(phonenumber)]
    combinations.append(wrap("+91" + phonenumber))
    combinations.append(wrap("91" + phonenumber))
    combinations.append(wrap("0" + phonenumber))
    combinations.append(wrap(phonenumber[:5] + " " + phonenumber[5:]))
    combinations.append(wrap("0" + phonenumber[:5] + " " + phonenumber[5:]))
    combinations.append(wrap("0" + phonenumber[:3] + " " + phonenumber[3:6] + " " + phonenumber[6:]))
    return combinations


def wrap(val):
    return '"' + val + '"'


def get_score_and_content(fraud_df):
    usernames = {''}
    content = ""
    urls = ""
    count = 0
    verified_count = 0
    followers_count = 0
    for _, row in fraud_df.iterrows():
        if row[common.USERNAME] not in usernames:
            content = content + row[common.TEXT] + "\n" + "******************\n"
            urls = urls + "@" + row[common.USERNAME] + " - " + str(row[common.DATE]) + " - " + row[
                common.TWEET_URL] + "\n"
            usernames.add(row[common.USERNAME])
            count = count + 1
            if row[common.VERIFIED]:
                verified_count += common.VERIFIED_USER_WEIGHTAGE
            followers_count += int(row[common.FOLLOWER_COUNT] / common.FOLLOWER_COUNT_WEIGHTAGE)
    return count, verified_count, followers_count, content, urls


def get_fraud_tier(score):
    if score <= 5:
        return common.LOW_TIER
    if score <= 50:
        return common.MEDIUM_TIER
    if score <= 100:
        return common.HIGH_TIER

    return common.VERY_HIGH_TIER
