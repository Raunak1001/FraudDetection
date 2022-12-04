import pandas as pd
import re
import common
import phonenumbers

emailRegex = "^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
vpaRegex = "^([a-zA-Z0-9]+([\-.]*[a-zA-Z0-9]+)*)@([a-zA-Z0-9]+([\-.]{1}[a-zA-Z0-9]+)*)$"
whiteList = ["7","8","9"]

def get_fraud_data(rawTweets):
    result_data = []
    unique_phone_numbers = []
    unique_fraud_mails = []
    unique_fraud_vpas = []
    print("Processing",len(rawTweets.index),"tweets")
    for index, row in rawTweets.iterrows():
        if index % 1000 == 0:
            print("Extracting",index,"th row")
        text = row[common.TEXT]
        phone_numbers = phonenumbers.PhoneNumberMatcher(text, "IN")
        for phone_number in phone_numbers:
            if phone_number.number.national_number in unique_phone_numbers:
                continue
            if str(phone_number.number.national_number)[:1:] not in whiteList:
                continue
            result_data.append([common.PHONE_NUMBER, phone_number.number.national_number, common.TWITTER, 0,'', 0, 0, 0, '', ''])
            unique_phone_numbers.append(phone_number.number.national_number)

        fraud_mails = get_attributes_from_regex(emailRegex, text)
        if len(fraud_mails) > 0:
            for fraud_mail in fraud_mails:
                if fraud_mail in unique_fraud_mails:
                    continue
                result_data.append([common.EMAIL, fraud_mail, common.TWITTER, 0,'', 0, 0, 0, '', ''])
                unique_fraud_mails.append(fraud_mails)

        fraud_vpas = get_attributes_from_regex(vpaRegex, text)
        if len(fraud_vpas) > 0:
            for fraud_vpa in fraud_vpas:
                if fraud_vpa in unique_fraud_vpas:
                    continue
                result_data.append([common.VPA, fraud_vpa, common.TWITTER, 0,'', 0, 0, 0, '', ''])
                unique_fraud_vpas.append(fraud_vpa)
    return pd.DataFrame(result_data,
                        columns=[common.IDENTIFIER_TYPE, common.IDENTIFIER_VALUE, common.SOURCE, common.SCORE, common.ACCURACY, 
                        common.VERIFIED_SCORE, common.FOLLOWERS_SCORE, common.FINAL_SCORE, common.TWEET_URL, common.CONTENT])


def get_attributes_from_regex(regexPattern, input_string):
    re.compile(regexPattern)
    matches = re.finditer(regexPattern, input_string, re.MULTILINE)
    attributes = []
    for match in matches:
        attributes.append(match.group())
    return attributes
