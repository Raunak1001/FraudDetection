from data_scrapper.twitter import scrapper
import common

def process_data(extracted_df):
    for ind,row in extracted_df.iterrows():
        fraud_df = get_fraud_info(row[common.IDENTIFIER_VALUE])
        extracted_df[common.SCORE][ind] = len(fraud_df.index)
    return extracted_df
        
def get_fraud_info(identifier):
    tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=[str(identifier)], from_time='', to_time='', operator=common.OPERATOR_AND, n=100)
    return tweets_df
