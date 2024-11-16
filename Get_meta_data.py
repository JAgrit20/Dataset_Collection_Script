import requests
import time
from pymongo import MongoClient

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

@rate_limited(120)
def get_bug_details(bug_id):
    url = f'https://bugzilla.mozilla.org/rest/bug/{bug_id}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # This will handle HTTP errors
        response_json = response.json()
        if 'bugs' in response_json and response_json['bugs']:
            return response_json['bugs'][0]
        else:
            print(f"No data found or 'bugs' key missing for Bug ID {bug_id}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bug details for Bug ID {bug_id}: {e}")
        return None


@rate_limited(120)
def get_bug_comments(bug_id):
    url = f'https://bugzilla.mozilla.org/rest/bug/{bug_id}/comment'
    try:
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        
        # Check if 'bugs' key exists in the response
        if 'bugs' in response_json and str(bug_id) in response_json['bugs']:
            comments = response_json['bugs'][str(bug_id)]['comments']
            for i, comment in enumerate(comments):
                comment['Bug report'] = True if i == 0 else False
            return comments
        else:
            print(f"No comments found or 'bugs' key missing for Bug ID {bug_id}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching comments for Bug ID {bug_id}: {e}")
    return []


@rate_limited(120)
def fetch_author_id(email):
    url = f'https://bugzilla.mozilla.org/rest/user/{email}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        user_data = response.json()
        if 'users' in user_data and user_data['users']:
            return user_data['users'][0].get('id')
    except requests.exceptions.RequestException as e:
        print(f"Error fetching author ID for email {email}: {e}")
    return None

def connect_to_mongo():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["MSR"]
        return db["Bug_meta_data"], db["All_comments"]
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None

def main():
    start_bug_id = 1901452
    end_bug_id = 1801508
    step = -1
    bug_data_collection, comments_collection = connect_to_mongo()
    
    if bug_data_collection is None or comments_collection is None:
        print("Database connection could not be established.")
        return
    else:
        print("Database connected successfully.")

    total_bugs = start_bug_id - end_bug_id
    print(f"Total number of bugs to process: {total_bugs}")

    for bug_id in range(start_bug_id, end_bug_id, step):
        print(f"Processing Bug ID: {bug_id}")
        bug_data = get_bug_details(bug_id)
        contributor_emails = set()
        contributor_ids = set()
        
        if bug_data:
            comments = get_bug_comments(bug_id)
            if comments:
                for comment in comments:
                    email = comment['creator']
                    contributor_emails.add(email)
                    author_id = fetch_author_id(email)
                    if author_id is not None:
                        contributor_ids.add(author_id)
                        comment['author_id'] = author_id
            try:
                if comments:
                    comments_collection.insert_many(comments)
                    print(f"Comments inserted for Bug ID: {bug_id}")
                bug_data['Contributor_email'] = list(contributor_emails)
                bug_data['Contributor_Id'] = list(contributor_ids)
                print(f"Contributor_Id for email {contributor_ids}")

                bug_data_collection.insert_one(bug_data)
                print(f"Bug data inserted for Bug ID: {bug_id}")
            except Exception as e:
                print(f"Failed to insert data for Bug ID {bug_id}: {e}")
        else:
            print(f"No data found for Bug ID: {bug_id}")

    print("Data has been saved to MongoDB.")

if __name__ == "__main__":
    main()
