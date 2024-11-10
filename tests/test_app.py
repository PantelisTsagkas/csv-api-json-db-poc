import pytest
import os
import pandas as pd
import json
import sqlite3
from unittest.mock import patch
from app import load_csv, make_api_calls, save_json, store_in_db

# Constants for testing
TEST_CSV_FILE = 'data/test_users.csv'
TEST_JSON_FILE = 'data/test_users_data.json'
TEST_DB_FILE = 'data/test_users.db'
API_URL = 'https://jsonplaceholder.typicode.com/posts'

@pytest.fixture
def sample_csv():
    """Create a sample CSV file for testing."""
    data = {
        'id': [1, 2],
        'name': ['John Doe', 'Jane Smith'],
        'username': ['johndoe', 'janesmith'],
        'email': ['john@example.com', 'jane@example.com']
    }
    df = pd.DataFrame(data)
    df.to_csv(TEST_CSV_FILE, index=False)
    yield
    # Teardown
    if os.path.exists(TEST_CSV_FILE):
        os.remove(TEST_CSV_FILE)

@pytest.fixture
def cleanup_files():
    """Cleanup JSON and DB files after tests."""
    yield
    if os.path.exists(TEST_JSON_FILE):
        os.remove(TEST_JSON_FILE)
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

def test_load_csv(sample_csv):
    """Test loading CSV data."""
    df = load_csv(TEST_CSV_FILE)
    assert df is not None
    assert len(df) == 2
    assert list(df.columns) == ['id', 'name', 'username', 'email']

@patch('app.requests.get')
def test_make_api_calls(mock_get, sample_csv, cleanup_files):
    """Test making API calls with mocked responses."""
    # Mock API responses
    mock_response_1 = {
        "userId": 1,
        "id": 1,
        "title": "Post 1",
        "body": "Content of post 1"
    }
    mock_response_2 = {
        "userId": 2,
        "id": 2,
        "title": "Post 2",
        "body": "Content of post 2"
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.side_effect = [[mock_response_1], [mock_response_2]]

    df = load_csv(TEST_CSV_FILE)
    posts = make_api_calls(df)
    assert len(posts) == 2
    assert posts[0]['title'] == "Post 1"
    assert posts[1]['title'] == "Post 2"

def test_save_json(cleanup_files):
    """Test saving data to JSON file."""
    sample_data = [
        {"userId": 1, "id": 1, "title": "Post 1", "body": "Content of post 1"},
        {"userId": 2, "id": 2, "title": "Post 2", "body": "Content of post 2"}
    ]
    save_json(sample_data, TEST_JSON_FILE)
    assert os.path.exists(TEST_JSON_FILE)
    with open(TEST_JSON_FILE, 'r') as f:
        data = json.load(f)
    assert len(data) == 2
    assert data[0]['title'] == "Post 1"

def test_store_in_db(cleanup_files):
    """Test storing data into SQLite database."""
    sample_data = [
        {"userId": 1, "id": 1, "title": "Post 1", "body": "Content of post 1"},
        {"userId": 2, "id": 2, "title": "Post 2", "body": "Content of post 2"}
    ]
    store_in_db(sample_data, TEST_DB_FILE)
    assert os.path.exists(TEST_DB_FILE)
    conn = sqlite3.connect(TEST_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM posts")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 2

