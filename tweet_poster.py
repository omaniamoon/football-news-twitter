#!/usr/bin/env python3
import os
import tweepy
import psycopg2
from datetime import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
              'host': os.getenv('DB_HOST'),
              'port': int(os.getenv('DB_PORT', 5432)),
              'database': os.getenv('DB_NAME'),
              'user': os.getenv('DB_USER'),
              'password': os.getenv('DB_PASSWORD')
}

# Twitter API credentials
TWITTER_API_KEY = os.getenv('TWITTER_CONSUMER_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_CONSUMER_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')

def get_db_connection():
              """Connect to PostgreSQL database"""
              try:
                                conn = psycopg2.connect(**DB_CONFIG)
                                logger.info("✅ Connected to PostgreSQL successfully")
                                return conn
except Exception as e:
        logger.error(f"❌ Database connection error: {e}")
        return None

def get_twitter_client():
              """Initialize Twitter API client"""
              try:
                                client = tweepy.Client(
                                                      consumer_key=TWITTER_API_KEY,
                                                      consumer_secret=TWITTER_API_SECRET,
                                                      access_token=TWITTER_ACCESS_TOKEN,
                                                      access_token_secret=TWITTER_ACCESS_TOKEN_SECRET
                                )
                                logger.info("✅ Twitter client initialized")
                                return client
except Exception as e:
        logger.error(f"❌ Twitter client error: {e}")
        return None

def get_pending_tweet(conn):
              """Get the oldest pending tweet from queue"""
              try:
                                cursor = conn.cursor()
                                cursor.execute("""
                                    SELECT id, text, scheduled_at
                                    FROM tweet_queue
                                    WHERE status = 'pending'
                                    AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                                    ORDER BY created_at ASC
                                    LIMIT 1
                                """)
                                result = cursor.fetchone()
                                cursor.close()

        if result:
                              logger.info(f"📥 Found pending tweet: ID {result[0]}")
                              return {'id': result[0], 'text': result[1], 'scheduled_at': result[2]}
else:
                      logger.info("ℹ️ No pending tweets in queue")
                      return None
except Exception as e:
        logger.error(f"❌ Error fetching tweet: {e}")
        return None

def update_tweet_status(conn, tweet_id, status, twitter_id=None, error_message=None):
              """Update tweet status in database"""
              try:
                                cursor = conn.cursor()
                                if status == 'posted':
                                                      cursor.execute("""
                                                                      UPDATE tweet_queue
                                                                                      SET status = %s, posted_at = NOW(), twitter_id = %s
                                                                                                      WHERE id = %s
                                                                                                                  """, (status, twitter_id, tweet_id))
              else:
                                    cursor.execute("""
                                                    UPDATE tweet_queue
                                                                    SET status = %s, error_message = %s, attempt_count = attempt_count + 1
                                                                                    WHERE id = %s
                                                                                                """, (status, error_message, tweet_id))

                  conn.commit()
        cursor.close()
        logger.info(f"✅ Updated tweet {tweet_id} status to: {status}")
except Exception as e:
        logger.error(f"❌ Error updating status: {e}")
        conn.rollback()

def post_tweet(client, conn, tweet_data):
              """Post tweet to Twitter"""
    try:
                      tweet_text = tweet_data['text']
                      tweet_id = tweet_data['id']

        # Post tweet
                      response = client.create_tweet(text=tweet_text)
                      twitter_id = response.data['id']

        logger.info(f"🎉 Tweet posted successfully! Twitter ID: {twitter_id}")

        # Update database
        update_tweet_status(conn, tweet_id, 'posted', twitter_id=twitter_id)
        return True

except tweepy.TweepyException as e:
        logger.error(f"❌ Twitter API error: {e}")
        update_tweet_status(conn, tweet_id, 'failed', error_message=str(e))
        return False
except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        update_tweet_status(conn, tweet_id, 'failed', error_message=str(e))
        return False

def main():
              """Main function - process one tweet"""
    logger.info("🚀 Starting tweet poster...")

    # Connect to database
    conn = get_db_connection()
    if not conn:
                      logger.error("Cannot proceed without database connection")
                      return

    # Initialize Twitter client
    twitter_client = get_twitter_client()
    if not twitter_client:
                      logger.error("Cannot proceed without Twitter client")
                      conn.close()
                      return

    # Get pending tweet
    tweet_data = get_pending_tweet(conn)

    if tweet_data:
                      # Post the tweet
                      success = post_tweet(twitter_client, conn, tweet_data)

        if success:
                              logger.info("✅ Tweet posted successfully!")
else:
            logger.error("❌ Failed to post tweet")
else:
        logger.info("ℹ️ No tweets to post at this time")

    # Close connection
    conn.close()
    logger.info("👋 Tweet poster finished")

if __name__ == "__main__":
              main()
