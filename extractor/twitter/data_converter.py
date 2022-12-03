from data_scrapper.twitter import scrapper
import pandas as pd
import re

phoneNumberRegex = '^(?:(?:\+|0{0,2})91(\s*[\ -]\s*)?|[0]?)?[789]\d{9}|(\d[ -]?){10}\d$'
emailRegex = "^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
vpaRegex = "^([a-zA-Z0-9]+([\-.]*[a-zA-Z0-9]+)*)@([a-zA-Z0-9]+([\-.]{1}[a-zA-Z0-9]+)*)$"

def get_fraud_data(rawTweets) -> pd.DataFrame:
    for _, row in rawTweets.iterrows():
        text = row['Text']
        c = re.compile(phoneNumberRegex)
        test = re.findall(c, text)
        print(test)
        if len(test) > 0:
            print(text)
            break
        # for i in phoneNumbers:
        #     print(text)
        #     print('phone', phoneNumbers)
        #     break
        # emails = re.findall(emailRegex, text)
        # for i in emails:
        #     print(text)
        #     print('mail', emails)
        #     break
        # VPAs = re.findall(vpaRegex, text)
        # for i in VPAs:
        #     print(text)
        #     print('vpa', VPAs)
        #     break
    return None