import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 
import boto3
from io import StringIO


def run_twitter_etl():

    access_key = "YOUR ACCESS KEY" 
    access_secret = "YOUR ACCESS SECRET KEY" 
    consumer_key = "YOUR CONSUMER KEY"
    consumer_secret = "YOUR CONSUMER SECRET KEY"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    
    # Replace these with your AWS credentials and S3 bucket information
    aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
    aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
    bucket_name = 'YOUR_BUCKET_NAME'
    file_name = 'elon_musk_tweets.csv'  # Name for the file in S3
    
    # Convert the DataFrame to CSV format as a string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Create a Boto3 S3 client
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    # Upload the CSV content to S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_content)

    print(f'DataFrame saved to S3 bucket: {bucket_name}/{file_name}')


