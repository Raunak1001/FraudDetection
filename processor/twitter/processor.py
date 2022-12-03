from data_scrapper.twitter import scrapper
import common

def process_data(extracted_df):
    tweets = []
    scores = []
    for ind,row in extracted_df.iterrows():
        fraud_df = get_fraud_info(get_number_combinations(str(row[common.IDENTIFIER_VALUE])))
        score, content = get_score_and_content(fraud_df)
        tweets.append(content)
        scores.append(score)
    extracted_df[common.CONTENT] = tweets
    extracted_df[common.SCORE] = scores  
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
    combinations.append(wrap("0"+phonenumber[:5]+" "+phonenumber[5:]))
    combinations.append(wrap("0"+phonenumber[:3]+" "+phonenumber[3:6]+" "+phonenumber[6:]))
    return combinations

def wrap(val):
    return '"'+val+'"'

def get_score_and_content(fraud_df):
    usernames = {''}
    content = ""
    for _,row in fraud_df.iterrows():
        if row[common.USERNAME] not in usernames:
            content = content+row[common.TEXT]+"\n"+"******************\n"
            usernames.add(row[common.USERNAME])
    return len(usernames)-1, content
