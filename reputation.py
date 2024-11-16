import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient

# Rate limit decorator
def rate_limited(max_per_minute):
    min_interval = 60.0 / float(max_per_minute)
    def decorate(func):
        last_time_called = [0.0]
        def rate_limited_function(*args, **kwargs):
            elapsed = time.time() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            last_time_called[0] = time.time()
            return func(*args, **kwargs)
        return rate_limited_function
    return decorate

# MongoDB connection setup
def connect_to_mongo(collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["MSR"]
    return db[collection_name]

# Function to fetch user details
@rate_limited(200)
def fetch_user_details(author_id):
    if not author_id:
        print("Author ID is NaN or invalid, skipping.")
        return None

    url = f'https://bugzilla.mozilla.org/user_profile?user_id={author_id}'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            # Extract required details
            user_details = {
                "User ID": author_id,
                "User Name": soup.select_one('div.vcard a').text.strip(),
                "Created On": soup.select_one('tr:contains("Created") td:last-of-type').text.strip(),
                "Last Activity": soup.select_one('tr:contains("Last activity") td:last-of-type').text.strip(),
                "Permissions": soup.select_one('tr:contains("Permissions") td:last-of-type').text.strip(),
                "Bugs Filed": int(soup.select_one('tr:contains("Bugs filed") td.numeric').text.strip()),
                "Comments Made": int(soup.select_one('tr:contains("Comments made") td.numeric').text.strip()),
                "Assigned to": int(soup.select_one('tr:contains("Assigned to") td.numeric').text.strip()),
                "Assigned to and Fixed": int(soup.select_one('tr:contains("Assigned to and fixed") td.numeric').text.strip()),
                "Commented on": int(soup.select_one('tr:contains("Commented on") td.numeric').text.strip()),
                "QA Contact": int(soup.select_one('tr:contains("QA-Contact") td.numeric').text.strip()),
                "Patches Submitted": int(soup.select_one('tr:contains("Patches submitted") td.numeric').text.strip()),
                "Patches Reviewed": int(soup.select_one('tr:contains("Patches reviewed") td.numeric').text.strip()),
                "Bugs Poked": int(soup.select_one('tr:contains("Bugs poked") td.numeric').text.strip())
            }
            return user_details
        except Exception as e:
            print(f"Error processing data for user_id {author_id}: {e}")
            return None
    else:
        print(f"Failed to retrieve the webpage for user_id {author_id}. Status code: {response.status_code}")
        return None

# Connect to the MongoDB collections
bug_reports_collection = connect_to_mongo("Bug_meta_data")
reputation_collection = connect_to_mongo("Reporter Reputation")

# Fetch contributor IDs from the bug reports collection
bug_reports = bug_reports_collection.find()

for report in bug_reports:
    contributor_ids = report.get("Contributor_Id", [])
    for author_id in contributor_ids:
        user_details = fetch_user_details(author_id)
        if user_details:
            # Insert user details into the reputation collection
            reputation_collection.insert_one(user_details)
            print(f"Inserted reputation data for Author ID: {author_id}")

print("Data processing completed and saved to MongoDB.")
