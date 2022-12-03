import pandas as pd
import re
import common
import phonenumbers

emailRegex = "^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
vpaRegex = "^([a-zA-Z0-9]+([\-.]*[a-zA-Z0-9]+)*)@([a-zA-Z0-9]+([\-.]{1}[a-zA-Z0-9]+)*)$"

def get_fraud_data(rawTweets):
    result_data = []
    for _, row in rawTweets.iterrows():
        text = row[common.TEXT]
        print(text)
        phone_numbers = phonenumbers.PhoneNumberMatcher(text, "IN")
        for phone_number in phone_numbers:
            print(phone_number)
            result_data.append([common.PHONE_NUMBER, phone_number.number.national_number, common.TWITTER])
        
        fraud_mails = get_attributes_from_regex(emailRegex, text)
        if len(fraud_mails)>0:
            for fraud_mail in fraud_mails:
                result_data.append([common.EMAIL, fraud_mail, common.TWITTER])
        
        fraud_vpas = get_attributes_from_regex(vpaRegex, text)
        if len(fraud_vpas)>0:
            for fraud_vpa in fraud_vpas:
                result_data.append([common.VPA, fraud_vpa, common.TWITTER])
    return pd.DataFrame(result_data, columns=[common.IDENTIFIER_TYPE, common.IDENTIFIER_VALUE,common.SOURCE])

def get_attributes_from_regex(regexPattern, input_string):
    re.fullmatch
    matches = re.finditer(regexPattern, input_string, re.MULTILINE)
    attributes = []
    for match in matches:
        attributes.append(match.group())
    return attributes