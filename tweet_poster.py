  import os, tweepy, psycopg2, logging
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def get_db():
          url = os.getenv("DATABASE_URL")
    if url:
                  return psycopg2.connect(url)
              return psycopg2.connect(host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 5432)), database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"))

def get_twitter():
          return tweepy.Client(bearer_token=os.getenv("TWITTER_BEARER_TOKEN"), consumer_key=os.getenv("TWITTER_CONSUMER_KEY"), consumer_secret=os.getenv("TWITTER_CONSUMER_SECRET"), access_token=os.getenv("TWITTER_ACCESS_TOKEN"), access_token_secret=os.getenv("TWITTER_ACCESS_TOKEN_SECRET"))

def main():
          logger.info("Starting...")
          try:
                        conn = get_db()
                        logger.info("DB connected")
except Exception as e:
        logger.error("DB failed: %s", e)
        return
    try:
                  cur = conn.cursor()
                  cur.execute("SELECT id, tweet_text FROM tweet_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
                  row = cur.fetchone()
                  cur.close()
                  if row is None:
                                    logger.info
