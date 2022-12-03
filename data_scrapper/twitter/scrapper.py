import snscrape.modules.twitter as sntwitter
import pandas as pd


def get_query_for_list(keywrods):
    if keywrods is None:
        return ''
    query = '('
    first = True
    for keyword in keywrods:
        if first:
            query = query + keyword
        else:
            query = query + ' OR ' + keyword
    query = query + ')'
    return query


def get_tweets(keywrods, usernames, from_time, to_time):
    attributes_container = []
    query = get_query_for_list(usernames)
    keyword_query = get_query_for_list(keywrods)
    if query == '':
        query = keyword_query
    elif keyword_query != '':
        query = query + ' OR ' + keyword_query

    if from_time != '':
        query = query + ' AND (since:' + from_time + ')'

    if to_time != '':
        query = query + ' AND (until:' + to_time + ')'
    print(query)
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i > 100:
            break
        attributes_container.append([tweet.date, tweet.likeCount, tweet.retweetCount, tweet.username, tweet.content])

    tweets_df = pd.DataFrame(attributes_container,
                             columns=["Date Created", "Number of Likes", "Retweet Count", "User Name", "Tweets"])

    print(tweets_df)


# get_tweets(["fraud", "scam"], ["Ankit"], '2022-07-06', '2022-07-08')
