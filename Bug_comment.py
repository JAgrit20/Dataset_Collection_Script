import requests
import csv
import pandas as pd
import os
import time

# URL template to fetch comments for a given bug ID
url_template = 'https://bugzilla.mozilla.org/rest/bug/{}/comment'

# Read bug IDs from the input CSV file
input_csv_path = 'remaining_7k_bugs.csv'
bug_ids = []
start_row = 1
total_rows_to_process = 7200

with open(input_csv_path, mode='r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for i, row in enumerate(reader):
        if i >= start_row and i < start_row + total_rows_to_process:
            bug_ids.append(row['Bug ID'])

print(f"Total bug IDs collected: {len(bug_ids)}")

# List to store the data
comments_data = []

# Loop through each bug ID and fetch comments
for count, bug_id in enumerate(bug_ids, start=1):
    try:
        print(f"Processing bug ID {bug_id} ({count}/{total_rows_to_process})...")
        response = requests.get(url_template.format(bug_id))
        response.raise_for_status()  # Check for HTTP request errors
        data = response.json()

        comments = data['bugs'][str(bug_id)]['comments']
        if comments:
            first_comment = comments[0]
            comments_data.append({
                'Bug ID': bug_id,
                'Comment ID': first_comment['id'],
                'Author': first_comment['creator'],
                'Comment Text': first_comment['text']
            })

            subsequent_comments_text = ' '.join(comment['text'] for comment in comments[1:])
            if subsequent_comments_text:
                comments_data.append({
                    'Bug ID': bug_id,
                    'Comment ID': 'subsequent_comments',
                    'Author': 'multiple',
                    'Comment Text': subsequent_comments_text
                })
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for Bug ID {bug_id}: {e}")
    except KeyError:
        print(f"KeyError: 'bugs' not found for Bug ID {bug_id}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    time.sleep(1)  # Reduce frequency of requests to avoid hitting rate limits

# Convert list of dicts to DataFrame
df_comments = pd.DataFrame(comments_data)

# Ensure the output directory exists
output_dir = 'Transfer_learning'
os.makedirs(output_dir, exist_ok=True)
output_csv_path = os.path.join(output_dir, 'all7k_Closed_Fixed_bug_comments_2_liner.csv')

# Save DataFrame to CSV
df_comments.to_csv(output_csv_path, index=False)
print(f"Data has been successfully written to {output_csv_path}")
