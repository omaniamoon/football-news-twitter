import os
import tweepy
import psycopg2
from datetime import datetime
from flask import Flask, jsonify, request
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)

# Database configuration
DB_CONFIG = {
      'host': os.getenv('DB_HOST', 'localhost'),
      'port': int(os.getenv('DB_PORT', 5432)),
      'database': os.getenv('DB_NAME', 'tweets'),
      'user': os.getenv('DB_USER', 'postgres'),
      'password': os.getenv('DB_PASSWORD', 'change_this')
}

# Twitter API configuration
TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN')
TWITTER_CONSUMER_KEY = os.getenv('TWITTER_CONSUMER_KEY')
TWITTER_CONSUMER_SECRET = os.getenv('TWITTER_CONSUMER_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')

# Initialize Tweepy Client
client = tweepy.Client(
      bearer_token=TWITTER_BEARER_TOKEN,
      consumer_key=TWITTER_CONSUMER_KEY,
      consumer_secret=TWITTER_CONSUMER_SECRET,
      access_token=TWITTER_ACCESS_TOKEN,
      access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
      wait_on_rate_limit=True
)

def get_db_connection():
      """Get database connection"""
      try:
                conn = psycopg2.connect(**DB_CONFIG)
                return conn
except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def get_pending_tweet():
      """Get next pending tweet from queue"""
      conn = get_db_connection()
      if not conn:
                return None

      cursor = conn.cursor()
      try:
                # Get oldest pending tweet
                cursor.execute("""
                            SELECT id, text FROM tweet_queue 
                                        WHERE status = 'pending' 
                                                    ORDER BY created_at ASC 
                                                                LIMIT 1
                                                                        """)
                tweet = cursor.fetchone()
                return tweet
except Exception as e:
        logger.error(f"Error getting pending tweet: {e}")
        return None
finally:
        cursor.close()
          conn.close()

def post_tweet(tweet_id, text):
      """Post tweet and update database"""
    conn = get_db_connection()
    if not conn:
              return False

    cursor = conn.cursor()
    try:
              # Create tweet
              response = client.create_tweet(text=text)
              twitter_id = response.data['id']

        # Update database
              cursor.execute("""
                  UPDATE tweet_queue 
                  SET status = 'posted', twitter_id = %s, posted_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                  WHERE id = %s
              """, (twitter_id, tweet_id))

        conn.commit()
        logger.info(f"SUCCESS: Tweet {tweet_id} posted - ID: {twitter_id}")
        return True

except Exception as e:
        logger.error(f"Error posting tweet: {e}")
        # Update as failed
        try:
                      cursor.execute("""
                                      UPDATE tweet_queue 
                                                      SET status = 'failed', error_message = %s, attempt_count = attempt_count + 1, updated_at = CURRENT_TIMESTAMP
                                                                      WHERE id = %s
                                                                                  """, (str(e), tweet_id))
                      conn.commit()
                  except:
            pass
                            return False
finally:
        cursor.close()
        conn.close()

@app.route('/health', methods=['GET'])
def health():
      """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/post-tweet', methods=['POST'])
def post_tweet_endpoint():
      """Endpoint to post pending tweets"""
    tweet = get_pending_tweet()
    if not tweet:
              return jsonify({'message': 'No pending tweets'}), 204

    tweet_id, text = tweet
    success = post_tweet(tweet_id, text)

    if success:
              return jsonify({'message': 'Tweet posted successfully', 'tweet_id': tweet_id}), 200
else:
        return jsonify({'message': 'Failed to post tweet', 'tweet_id': tweet_id}), 500

@app.route('/queue-status', methods=['GET'])
def queue_status():
      """Get queue status"""
    conn = get_db_connection()
    if not conn:
              return jsonify({'error': 'Database connection failed'}), 500

    cursor = conn.cursor()
    try:
              cursor.execute("""
                          SELECT 
                                          COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                                                          COUNT(CASE WHEN status = 'posted' THEN 1 END) as posted,
                                                                          COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                                                                                      FROM tweet_queue
                                                                                              """)
              pending, posted, failed = cursor.fetchone()
              return jsonify({
                  'pending': pending,
                  'posted': posted,
                  'failed': failed
              }), 200
except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return jsonify({'error': str(e)}), 500
finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
      port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
