#!/usr/bin/env python3
import os
import tweepy
import psycopg2
from datetime import datetime
from flask import Flask, request, jsonify
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

def get_db_connection():
          """Get database connection"""
          try:
                        conn = psycopg2.connect(**DB_CONFIG)
                        return conn
except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def get_pending_tweet():
          """Get the next pending tweet from database"""
          conn = get_db_connection()
          if not conn:
                        return None

          try:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT id, text FROM tweet_queue 
                            WHERE status = 'pending' 
                            ORDER BY scheduled_at ASC 
                            LIMIT 1
                        """)
                        result = cursor.fetchone()
                        cursor.close()
                        conn.close()

        if result:
                          return {'id': result[0], 'text': result[1]}
                      return None
except Exception as e:
        logger.error(f"Error getting pending tweet: {str(e)}")
        return None

def post_tweet(tweet_id, text):
          """Post tweet to Twitter"""
          try:
                        # Initialize Twitter client
                        client = tweepy.Client(
                                          bearer_token=TWITTER_BEARER_TOKEN,
                                          consumer_key=TWITTER_CONSUMER_KEY,
                                          consumer_secret=TWITTER_CONSUMER_SECRET,
                                          access_token=TWITTER_ACCESS_TOKEN,
                                          access_token_secret=TWITTER_ACCESS_TOKEN_SECRET
                        )

        # Post the tweet
              response = client.create_tweet(text=text)
        twitter_id = response.data['id']

        # Update database
        conn = get_db_connection()
        if conn:
                          cursor = conn.cursor()
                          cursor.execute("""
                              UPDATE tweet_queue 
                              SET status = 'posted', twitter_id = %s, posted_at = CURRENT_TIMESTAMP
                              WHERE id = %s
                          """, (twitter_id, tweet_id))
                          conn.commit()
                          cursor.close()
                          conn.close()

        logger.info(f"Tweet {tweet_id} posted successfully: {twitter_id}")
        return True

except Exception as e:
        # Update as failed
        conn = get_db_connection()
        if conn:
                          cursor = conn.cursor()
                          cursor.execute("""
                              UPDATE tweet_queue 
                              SET status = 'failed', error_message = %s, attempt_count = attempt_count + 1, updated_at = CURRENT_TIMESTAMP
                              WHERE id = %s
                          """, (str(e), tweet_id))
                          conn.commit()
                          cursor.close()
                          conn.close()

        logger.error(f"Error posting tweet {tweet_id}: {str(e)}")
        return False

@app.route('/post-tweet', methods=['POST'])
def api_post_tweet():
          """API endpoint to post a tweet"""
    try:
                  data = request.get_json()
                  text = data.get('text')

        if not text:
                          return jsonify({'error': 'Missing text'}), 400

        # Save to queue
        conn = get_db_connection()
        if not conn:
                          return jsonify({'error': 'Database error'}), 500

        cursor = conn.cursor()
        cursor.execute("""
                    INSERT INTO tweet_queue (text, status, created_at, scheduled_at)
                                VALUES (%s, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                            RETURNING id
                                                    """, (text,))
        tweet_id = cursor.fetchone()[0]
        conn.commit()

        # Try to post immediately
        if post_tweet(tweet_id, text):
                          cursor.close()
                          conn.close()
                          return jsonify({'status': 'posted', 'tweet_id': tweet_id}), 200
else:
                  cursor.close()
                  conn.close()
                  return jsonify({'status': 'queued', 'tweet_id': tweet_id}), 202

except Exception as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
          """Health check endpoint"""
          return jsonify({'status': 'healthy'}), 200

@app.route('/', methods=['GET'])
def home():
          """Home endpoint"""
          return jsonify({'message': 'Football News Twitter Poster Service', 'version': '1.0'}), 200

if __name__ == '__main__':
          port = int(os.getenv('PORT', 10000))
          app.run(host='0.0.0.0', port=port, debug=False)
