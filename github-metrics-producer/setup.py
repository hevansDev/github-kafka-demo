import clickhouse_connect
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
from confluent_kafka import Producer

from time import sleep
from threading import Thread

load_dotenv()

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost', 
    port=8123,
    username='default',
    password='ClickHousePassword'
)

print("Dropping existing prod* tables...",end="")

client.command("DROP TABLE  IF EXISTS prod_github_data")
client.command("DROP TABLE  IF EXISTS prod_github_data_kafka")
client.command("DROP TABLE  IF EXISTS prod_github_data_mv")

print("Done")

print("Ingesting historic data...")

sql_create_table="""
CREATE TABLE prod_github_data
(
    `repo` String,
    `date` DateTime,
    `stars_gained_that_day` UInt32,
    `prs_opened_that_day` UInt32,
    `cumulative_stars` UInt32,
    `cumulative_prs` UInt32
)
ENGINE = MergeTree
ORDER BY tuple(date)
"""

client.command(sql_create_table)

os.system('clickhouse-client --password ClickHousePassword --query "INSERT INTO prod_github_data FORMAT CSV" < ../data/2011-2015-kafka.csv')

def get_prs_by_date(repo, start_date, end_date, token=None):
    """Get PRs grouped by date with authentication"""
    print(f"Fetching PRs for {repo} from {start_date} to {end_date}")
    
    url = f"https://api.github.com/repos/{repo}/pulls"
    headers = {}
    
    if token:
        headers['Authorization'] = f'token {token}'
    
    prs_by_date = {}
    page = 1
    max_pages = 100
    
    while page <= max_pages:
        params = {
            'state': 'all',
            'sort': 'created',
            'direction': 'desc',
            'page': page,
            'per_page': 100
        }
        
        if page % 10 == 0:
            print(f"Fetching page {page}...")
        
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code != 200:
            print(f"API error: {response.status_code}")
            break
            
        prs = response.json()
        if not prs:
            break
            
        found_old_pr = False
        for pr in prs:
            created_date = pr['created_at'][:10]
            if start_date <= created_date <= end_date:
                prs_by_date[created_date] = prs_by_date.get(created_date, 0) + 1
            elif created_date < start_date:
                found_old_pr = True
                break
        
        if found_old_pr:
            print(f"Reached older PRs at page {page}")
            break
            
        page += 1
    
    print(f"Total PRs found: {sum(prs_by_date.values())}")
    return prs_by_date

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "null")
# Load your CSV
df = pd.read_csv("../data/apache-kafka-stars-history.csv")

# Convert date format and rename columns
df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
df = df.rename(columns={
    'day-stars': 'stars_gained_that_day',
    'total-stars': 'cumulative_stars'
})

# Get PR data starting from day AFTER the GitHub Archive ends
repo = "apache/kafka"
start_date = "2025-01-02"  # Start from Jan 2nd since GitHub Archive has Jan 1st
end_date = df['date'].max()

prs_by_date = get_prs_by_date(repo, start_date, end_date, GITHUB_TOKEN)

# Add columns
df['repo'] = repo
df['prs_opened_that_day'] = df['date'].map(prs_by_date).fillna(0).astype(int)

# For Jan 1st, use 0 PRs (since it's already in GitHub Archive)
df.loc[df['date'] == '2025-01-01', 'prs_opened_that_day'] = 0

# Calculate cumulative PRs starting from GitHub Archive end value
github_archive_last_cumulative_prs = 17239  # From your GitHub Archive data
df = df.sort_values('date')
df['cumulative_prs'] = github_archive_last_cumulative_prs + df['prs_opened_that_day'].cumsum()

# Reorder columns
df = df[['repo', 'date', 'stars_gained_that_day', 'prs_opened_that_day', 'cumulative_stars', 'cumulative_prs']]

# Save
df.to_csv('../data/2025-today-kafka.csv', index=False)

os.system('clickhouse-client --password ClickHousePassword --query "INSERT INTO prod_github_data FORMAT CSV" < ../data/2025-today-kafka.csv')

print("Done")

print("Creating kafka topic, table, and mv...")

def get_recent_activity(repo="apache/kafka", token=None):
   """Get new stars and PRs from the past hour"""
   
   headers = {}
   if token:
       headers['Authorization'] = f'token {token}'
   
   # Calculate 1 hour ago
   one_hour_ago = datetime.utcnow() - timedelta(hours=1)
   since_time = one_hour_ago.isoformat() + 'Z'
   
   # Get recent stargazers (limited data available)
   stars_url = f"https://api.github.com/repos/{repo}/stargazers"
   star_headers = headers.copy()
   star_headers['Accept'] = 'application/vnd.github.star+json'
   
   recent_stars = 0
   try:
       response = requests.get(stars_url, headers=star_headers, params={'per_page': 100})
       if response.status_code == 200:
           stargazers = response.json()
           for star in stargazers:
               if 'starred_at' in star and star['starred_at'] >= since_time:
                   recent_stars += 1
   except:
       recent_stars = "unavailable"
   
   # Get recent PRs
   prs_url = f"https://api.github.com/repos/{repo}/pulls"
   recent_prs = 0
   
   response = requests.get(prs_url, headers=headers, params={
       'state': 'all',
       'sort': 'created',
       'direction': 'desc',
       'per_page': 100
   })
   
   if response.status_code == 200:
       prs = response.json()
       for pr in prs:
           if pr['created_at'] >= since_time:
               recent_prs += 1
           else:
               break
   
   return {
       'repo': repo,
       'time_window': '1 hour',
       'new_stars': recent_stars,
       'new_prs': recent_prs,
       'checked_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
   }

# Get recent activity
def get_new_record():
    activity = get_recent_activity()
    print(f"In the past hour:")
    print(f"New stars: {activity['new_stars']}")
    print(f"New PRs: {activity['new_prs']}")
    print(f"Checked at: {activity['checked_at']}")

    result = client.query('SELECT cumulative_stars, cumulative_prs FROM prod_github_data ORDER BY date DESC LIMIT 1')
    cumulative_stars, cumulative_prs = result.result_rows[0]

    new_record = {"repo":"apache/kafka",
                "date":activity['checked_at'],
                "stars":activity['new_stars'],
                "prs":activity['new_prs'],
                "cumulative_stars":cumulative_stars+activity['new_stars'],
                "cumulative_prs":cumulative_prs+activity['new_prs']}

    producer = Producer({
            'bootstrap.servers': 'localhost:9092'
        })

    producer.produce("prod-github-metrics", json.dumps(new_record).encode('utf-8'))
    producer.flush()

get_new_record()

sql_create_table="""
CREATE TABLE prod_github_data_kafka
(
    `repo` String,
    `date` DateTime,
    `stars_gained_that_day` UInt32,
    `prs_opened_that_day` UInt32,
    `cumulative_stars` UInt32,
    `cumulative_prs` UInt32
)
ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'prod-github-metrics',
    kafka_group_name = 'prod_clickhouse_github_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
"""

client.command(sql_create_table)

sql_materialized_view="""
CREATE MATERIALIZED VIEW prod_github_data_mv TO prod_github_data AS
SELECT 
    repo,
    date,
    stars_gained_that_day,
    prs_opened_that_day,
    cumulative_stars,
    cumulative_prs
FROM prod_github_data_kafka;
"""

client.command(sql_materialized_view)

print("Done")

print("Producing new record ever hour")

if __name__ == '__main__':
    while True:
        sleep(1800)
        Thread(target = get_new_record).start()