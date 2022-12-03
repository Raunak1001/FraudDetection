import snscrape.modules.twitter as sntwitter
import pandas as pd

if __name__ == "__main__":
    # Created a list to append all tweet attributes(data)
    attributes_container = []
    query = '(phone OR number OR mobile OR no OR bank )(fraudster OR scammer OR cheat OR scam OR fraud OR loot ) (@CPMumbaiPolice  OR @cyberdost OR @noidapolice OR @uppolice OR @delhipolice OR @cybercrimeCID OR @mumbaipolice OR @dgpMaharashtra OR @mahaPolice OR @blrCityPolice OR @cybercellRaj OR @cybercellindia OR @DoT_India  OR @assampolice OR @wbpolice OR @kolkatapolice)'
    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i>100:
            break
        attributes_container.append([tweet.date, tweet.content])
        
    # Creating a dataframe from the tweets list above 
    tweets_df = pd.DataFrame(attributes_container, columns=["Date Created", "Tweets"])
    print(tweets_df)