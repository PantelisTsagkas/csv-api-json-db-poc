import pandas as pd
import requests
import json
import sqlite3
import os

# Constants
CSV_FILE = 'data/users.csv'
JSON_FILE = 'data/users_data.json'
DB_FILE = 'data/users.db'
API_URL = 'https://jsonplaceholder.typicode.com/posts'

def load_csv(file_path):
    """Load CSV data into a pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None

def make_api_calls(df):
    """Make API calls for each user and collect responses."""
    all_posts = []
    for index, row in df.iterrows():
        user_id = row['id']
        print(f"Fetching posts for user ID {user_id}...")
        response = requests.get(API_URL, params={'userId': user_id})
        if response.status_code == 200:
            posts = response.json()
            all_posts.extend(posts)
            print(f"Retrieved {len(posts)} posts for user ID {user_id}")
        else:
            print(f"Failed to fetch posts for user ID {user_id}. Status Code: {response.status_code}")
    return all_posts

def save_json(data, file_path):
    """Save data to a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Saved data to {file_path}")
    except Exception as e:
        print(f"Error saving JSON: {e}")

def store_in_db(data, db_path):
    """Store JSON data into a SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                userId INTEGER,
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT
            )
        ''')
        # Insert data
        for post in data:
            cursor.execute('''
                INSERT OR IGNORE INTO posts (userId, id, title, body)
                VALUES (?, ?, ?, ?)
            ''', (post['userId'], post['id'], post['title'], post['body']))
        conn.commit()
        conn.close()
        print(f"Stored {len(data)} posts into the database {db_path}")
    except Exception as e:
        print(f"Error storing data in DB: {e}")

def main():
    # Load CSV data
    df = load_csv(CSV_FILE)
    if df is None:
        return

    # Make API calls
    posts = make_api_calls(df)

    # Save JSON data
    save_json(posts, JSON_FILE)

    # Store data in DB
    store_in_db(posts, DB_FILE)

if __name__ == '__main__':
    main()

