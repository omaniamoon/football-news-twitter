#!/usr/bin/env python3
import os
import tweepy
import psycopg2
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
      """Connect to PostgreSQL using DATABASE_URL or individual vars"""
      database_url = os.getenv('DATABASE_URL')
      if database_url:
                return psycopg2.connect(database_url)
else:
        return psycopg2.connect(
                      host=os.getenv('DB_HOST'),
                      port=int(os.getenv('DB_PORT', 5432)),
                      database=os.getenv('DB_NAME'),
                      user=os.getenv('DB_USER'),
                      password=os.getenv('DB_PASSWORD')
        )

def get_twitter_client():
      """Create Twitter API v2 client"""
      client = tweepy.Client(
          bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
          consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
          consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
          access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
          access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
      )
      return client

def get_pending_tweet(conn):
      """Get the oldest pending tweet from queue"""
      cur = conn.cursor()
      cur.execute("""
          SELECT id, tweet_text FROM tweet_queue
          WHERE status = 'pending'
          ORDER BY created_at ASC
          LIMIT 1
      """)
      result = cur.fetchone()
      cur.close()
      return result

def mark_tweet_posted(conn, tweet_id, twitter_id):
      """Mark tweet as posted"""
      cur = conn.cursor()
      cur.execute("""
          UPDATE tweet_queue
          SET status = 'posted', posted_at = %s, twitter_id = %s
          WHERE id = %s
      """, (datetime.now(), str(twitter_id), tweet_id))
      conn.commit()
      cur.close()

def mark_tweet_failed(conn, tweet_id, error_msg):
      """Mark tweet as failed"""
      cur = conn.cursor()
      cur.execute("""
          UPDATE tweet_queue
          SET status = 'failed', error_message = %s,
              attempt_count = attempt_count + 1
          WHERE id = %s
      """, (str(error_msg), tweet_id))
      conn.commit()
      cur.close()

def main():
      logger.info("Tweet poster starting...")

    try:
              conn = get_db_connection()
              logger.info("Database connected!")
except Exception as e:
          logger.error(f"Database connection failed: {e}")
          return

    try:
              tweet_data = get_pending_tweet(conn)

        if tweet_data is None:
                      logger.info("No pending tweets in queue.")
                      conn.close()
                      return

        tweet_id, tweet_text = tweet_data
        logger.info(f"Found pending tweet ID {tweet_id}: {tweet_text[:50]}...")

        try:
                      client = get_twitter_client()
                      response = client.create_tweet(text=tweet_text)
                      twitter_id = response.data['id']
                      mark_tweet_posted(conn, tweet_id, twitter_id)
                      logger.info(f"Tweet posted! Twitter ID: {twitter_id}")
except Exception as e:
              mark_tweet_failed(conn, tweet_id, str(e))
              logger.error(f"Failed to post tweet: {e}")

except Exception as e:
          logger.error(f"Error: {e}")
finally:
          conn.close()
          logger.info("Done.")

if __name__ == "__main__":
      main()
  
