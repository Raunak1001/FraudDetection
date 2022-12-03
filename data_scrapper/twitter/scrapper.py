import snscrape.modules.twitter as sntwitter
import pandas as pd
import common


def get_query_for_list(keywords):
    if keywords is None:
        return ''
    query = '('
    first = True
    for keyword in keywords:
        if first:
            query = query + keyword
        else:
            query = query + ' OR ' + keyword
    query = query + ')'
    return query


def get_tweets(keywords1, keywords2=None, from_time=None, to_time=None, operator=None):
    if operator is None or operator == '':
        operator = common.OPERATOR_OR
    attributes_container = []
    query = get_query_for_list(keywords1)
    keyword_query = get_query_for_list(keywords2)
    if query == '':
        query = keyword_query
    elif keyword_query != '':
        query = query + ' ' + operator + ' ' + keyword_query

    if from_time is not None and from_time != '':
        query = query + ' AND (since:' + from_time + ')'

    if to_time is not None and to_time != '':
        query = query + ' AND (until:' + to_time + ')'
    print(query)
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i > 100:
            break
        attributes_container.append(
            [tweet.content, '', tweet.retweetCount, tweet.likeCount, tweet.replyCount, tweet.username, common.TWITTER])

    tweets_df = pd.DataFrame(attributes_container,
                             columns=[common.TEXT, common.IMAGE_URL, common.SHARE_COUNT, common.LIKE_COUNT,
                                      common.REPLY_COUNT, common.USERNMAE, common.PLATFROM])

    return tweets_df
