import snscrape.modules.twitter as sntwitter
import pandas as pd
import common
import threading

pd.options.mode.chained_assignment = None
pd.set_option('display.max_colwidth', None)

final_tweet_df = pd.DataFrame()
import requests
from PIL import Image
import pytesseract
import io


def get_query_for_list(keywords, prefix=''):
    if keywords is None:
        return ''
    query = '('
    first = True
    for keyword in keywords:
        if first:
            query = query + prefix + keyword
            first = False
        else:
            query = query + ' OR ' + prefix + keyword
    query = query + ')'
    return query


def get_tweets(keywords1, keywords2=None, from_time=None, to_time=None, operator=None, n=None, mentions=None):
    global final_tweet_df
    final_tweet_df = pd.DataFrame()
    if mentions is None or mentions == '' or len(mentions) == 0:
        scrape_tweets(keywords1, keywords2, from_time, to_time, operator, n, mentions)
        return final_tweet_df

    i = 0
    threads = []
    while i < len(mentions):
        j = min(i + common.MENTIONS_BATCH, len(mentions))
        t = threading.Thread(target=scrape_tweets,
                             args=(keywords1, keywords2, from_time, to_time, operator, n, mentions[i:j],))
        threads.append(t)
        i = j
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return final_tweet_df


def scrape_tweets(keywords1, keywords2=None, from_time=None, to_time=None, operator=None, n=None, mentions=None):
    print("Running")
    global final_tweet_df
    if operator is None or operator == '':
        operator = common.OPERATOR_OR
    if n is None:
        n = 10000
    attributes_container = []
    query = get_query_for_list(keywords1)
    keyword_query = get_query_for_list(keywords2)
    if query == '':
        query = keyword_query
    elif keyword_query != '':
        query = query + ' ' + operator + ' ' + keyword_query

    if mentions is not None:
        query = '(' + query + ')'
        mention_query = get_query_for_list(mentions, '@')
        query = query + ' AND ' + mention_query
        mention_query = get_query_for_list(mentions, '-from:')
        query = query + ' AND ' + mention_query

    if from_time is not None and from_time != '':
        query = query + ' AND (since:' + from_time + ')'

    if to_time is not None and to_time != '':
        query = query + ' AND (until:' + to_time + ')'

    # query = query + 'AND (filter:verified)'

    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i > n:
            break
        
        url = ""
        try:
            url = tweet.media[0].fullUrl # .previewUrl if you want previewUrl
        except:
            pass

        attributes_container.append(
            [tweet.content, url, tweet.retweetCount, tweet.likeCount, tweet.replyCount, tweet.user.username, common.TWITTER,
             tweet.date, tweet.user.verified, tweet.user.followersCount])

    tweets_df = pd.DataFrame(attributes_container,
                             columns=[common.TEXT, common.IMAGE_URL, common.SHARE_COUNT, common.LIKE_COUNT,
                                      common.REPLY_COUNT, common.USERNAME, common.PLATFROM, common.DATE, common.VERIFIED, common.FOLLOWER_COUNT])
    # pd.concat(final_tweet_df, tweets_df)
    final_tweet_df = final_tweet_df.append(tweets_df, ignore_index=True)
    return


def scrape_tweets_sync(keywords1, keywords2=None, from_time=None, to_time=None, operator=None, n=None, mentions=None):
    if operator is None or operator == '':
        operator = common.OPERATOR_OR
    if n is None:
        n = 10000
    attributes_container = []
    query = get_query_for_list(keywords1)
    keyword_query = get_query_for_list(keywords2)
    if query == '':
        query = keyword_query
    elif keyword_query != '':
        query = query + ' ' + operator + ' ' + keyword_query

    if mentions is not None:
        query = '(' + query + ')'
        mention_query = get_query_for_list(mentions, '@')
        query = query + ' AND ' + mention_query
        mention_query = get_query_for_list(mentions, '-from:')
        query = query + ' AND ' + mention_query

    if from_time is not None and from_time != '':
        query = query + ' AND (since:' + from_time + ')'

    if to_time is not None and to_time != '':
        query = query + ' AND (until:' + to_time + ')'

    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i > n:
            break
        attributes_container.append(
            [tweet.content, '', tweet.retweetCount, tweet.likeCount, tweet.replyCount, tweet.user.username, common.TWITTER,
             tweet.date, tweet.user.verified, tweet.user.followersCount, tweet.url])

    tweets_df = pd.DataFrame(attributes_container,
                             columns=[common.TEXT, common.IMAGE_URL, common.SHARE_COUNT, common.LIKE_COUNT,
                                      common.REPLY_COUNT, common.USERNAME, common.PLATFROM, common.DATE, common.VERIFIED, common.FOLLOWER_COUNT, common.TWEET_URL])
    return tweets_df

def fill_media_content(tweets_df):
    if not common.EXTRACT_MEDIA_CONTENT:
        return tweets_df
    
    threads = []
    for ind, row in tweets_df.iterrows():
        url = tweets_df[common.IMAGE_URL][ind]
        if url=='' or url==None:
            continue
        t = threading.Thread(target=extract_data_from_media, args=(url, tweets_df, ind))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    return tweets_df


def extract_data_from_media(url, tweets_df, ind):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    img = Image.open(io.BytesIO(r.content))
    text = tweets_df[common.TEXT][ind]+"\n"+pytesseract.image_to_string(img)
    tweets_df[common.TEXT][ind] = text
