import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import math
from pymongo import MongoClient

# MongoDB connection setup
def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["MSR"]
    collection = db["Reporter Reputation"]
    return collection

# Function to fetch user details
def fetch_user_details(author_id):
    if pd.isna(author_id):
        print("Author ID is NaN, skipping.")
        return None
    
    try:
        user_id = int(author_id)
    except ValueError as e:
        print(f"Invalid Author ID {author_id}: {e}")
        return None

    url = f'https://bugzilla.mozilla.org/user_profile?user_id={user_id}'
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            # Extract required details
            user_details = {
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
            print(f"Error processing data for user_id {user_id}: {e}")
            return None
    else:
        print(f"Failed to retrieve the webpage for user_id {user_id}. Status code:", response.status_code)
        return None

# Read CSV file starting from the 1200th row
data = pd.read_csv('WITH_id_bug_comments_fixed_final.csv', skiprows=range(1, 1199))
total_rows = len(data)
print(f"Total records to process: {total_rows}")

# Connect to MongoDB collection
collection = connect_to_mongo()

# Iterate over rows in DataFrame
for index, row in data.iterrows():
    print(row)
    user_details = fetch_user_details(row['Author ID'])
    if user_details:
        # Combine row data with fetched user details and insert into MongoDB
        document = {**row.to_dict(), **user_details}
        collection.insert_one(document)
    
    # Progress update and pause every 10 records to avoid overloading
    completed = index + 1
    progress_percentage = (completed / total_rows) * 100
    print(f"Processed {completed}/{total_rows} records ({progress_percentage:.2f}%)")

    if completed % 10 == 0:
        print(f"Data inserted to MongoDB after processing {completed} records.")
        time.sleep(10)  # Pausing to respect any rate limits

print("Data processing completed and saved to MongoDB.")
