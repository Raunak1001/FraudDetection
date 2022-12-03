from data_scrapper.twitter import scrapper
import common

def process_data(extracted_df):
    for ind,row in extracted_df.iterrows():
        fraud_df = get_fraud_info(get_number_combinations(str(row[common.IDENTIFIER_VALUE])))
        extracted_df[common.SCORE][ind] = get_score(fraud_df)
    return extracted_df
        
def get_fraud_info(identifier):
    tweets_df = scrapper.get_tweets(keywords1=common.KEYWORDS, keywords2=identifier, from_time='', to_time='', operator=common.OPERATOR_AND, n=100)
    return tweets_df

def get_number_combinations(phonenumber):
    combinations = [wrap(phonenumber)]
    combinations.append(wrap("+91"+phonenumber))
    combinations.append(wrap("91"+phonenumber))
    combinations.append(wrap("0"+phonenumber))
    combinations.append(wrap(phonenumber[:5]+" "+phonenumber[5:]))
    return combinations

def wrap(val):
    return '"'+val+'"'

def get_score(fraud_df):
    usernames = {''}
    for _,row in fraud_df.iterrows():
        usernames.add(row[common.USERNAME])
    return len(usernames)-1
