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

# ===== CONFIGURATION =====
GRANULARITY_CONFIG = {
    'interval_minutes': 60,  # How often to collect data (60 = hourly, 30 = every 30 min, etc.)
    'lookback_hours': 1,     # How far back to look for new activity
    'table_prefix': 'prod'   # Prefix for table names
}

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost', 
    port=8123,
    username='default',
    password='ClickHousePassword'
)

def get_table_names(prefix):
    """Generate table names with prefix"""
    return {
        'main': f"{prefix}_github_data",
        'kafka': f"{prefix}_github_data_kafka", 
        'mv': f"{prefix}_github_data_mv",
        'topic': f"{prefix}-github-metrics"
    }

def setup_tables(prefix):
    """Create tables with specified prefix"""
    tables = get_table_names(prefix)
    
    print(f"Dropping existing {prefix}* tables...", end="")
    client.command(f"DROP TABLE IF EXISTS {tables['main']}")
    client.command(f"DROP TABLE IF EXISTS {tables['kafka']}")
    client.command(f"DROP TABLE IF EXISTS {tables['mv']}")
    print("Done")

    print("Creating main table...")
    sql_create_table = f"""
    CREATE TABLE {tables['main']}
    (
        `repo` String,
        `date` DateTime,
        `stars_gained_that_period` UInt32,
        `prs_opened_that_period` UInt32,
        `cumulative_stars` UInt32,
        `cumulative_prs` UInt32
    )
    ENGINE = MergeTree
    ORDER BY tuple(date)
    """
    client.command(sql_create_table)
    return tables

def ingest_historic_data(tables):
    """Ingest historic data"""
    print("Ingesting historic data...")
    
    # Ingest 2011-2015 data
    os.system('tail -n +2 ../data/2011-2015-kafka.csv | clickhouse-client --password ClickHousePassword --query "INSERT INTO prod_github_data (repo, date, stars_gained_that_period, prs_opened_that_period, cumulative_stars, cumulative_prs) SELECT repo, toDate(date), stars_gained_that_period, prs_opened_that_period, cumulative_stars, cumulative_prs FROM input(\'repo String, date String, stars_gained_that_period UInt32, prs_opened_that_period UInt32, cumulative_stars UInt32, cumulative_prs UInt32\') FORMAT CSV"')
    
    # Process recent data with PR enrichment
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "null")
    df = pd.read_csv("../data/apache-kafka-stars-history.csv")
    
    # Convert date format and rename columns
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
    df = df.rename(columns={
        'day-stars': 'stars_gained_that_period',
        'total-stars': 'cumulative_stars'
    })
    
    # Get PR data
    repo = "apache/kafka"
    start_date = "2025-01-02"
    end_date = df['date'].max()
    
    prs_by_date = get_prs_by_date(repo, start_date, end_date, GITHUB_TOKEN)
    
    # Add columns
    df['repo'] = repo
    df['prs_opened_that_period'] = df['date'].map(prs_by_date).fillna(0).astype(int)
    df.loc[df['date'] == '2025-01-01', 'prs_opened_that_period'] = 0
    
    # Calculate cumulative PRs
    github_archive_last_cumulative_prs = 17239
    df = df.sort_values('date')
    df['cumulative_prs'] = github_archive_last_cumulative_prs + df['prs_opened_that_period'].cumsum()
    
    # Reorder columns
    df = df[['repo', 'date', 'stars_gained_that_period', 'prs_opened_that_period', 'cumulative_stars', 'cumulative_prs']]
    
    # Save and ingest
    df.to_csv('../data/2025-today-kafka.csv', index=False)
    os.system('tail -n +2 ../data/2025-today-kafka.csv | clickhouse-client --password ClickHousePassword --query "INSERT INTO prod_github_data (repo, date, stars_gained_that_period, prs_opened_that_period, cumulative_stars, cumulative_prs) SELECT repo, toDate(date), stars_gained_that_period, prs_opened_that_period, cumulative_stars, cumulative_prs FROM input(\'repo String, date String, stars_gained_that_period UInt32, prs_opened_that_period UInt32, cumulative_stars UInt32, cumulative_prs UInt32\') FORMAT CSV"')
    print("Done")

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

def get_recent_activity(repo="apache/kafka", token=None, lookback_hours=1):
    """Get new stars and PRs from the specified time period"""
    
    headers = {}
    if token:
        headers['Authorization'] = f'token {token}'
    
    # Calculate lookback time
    lookback_time = datetime.utcnow() - timedelta(hours=lookback_hours)
    since_time = lookback_time.isoformat() + 'Z'
    
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
        recent_stars = 0  # Changed from "unavailable" to 0 for numeric consistency
    
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
        'time_window': f'{lookback_hours} hour(s)',
        'new_stars': recent_stars,
        'new_prs': recent_prs,
        'checked_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'lookback_hours': lookback_hours
    }

def create_kafka_infrastructure(tables):
    """Create Kafka table and materialized view"""
    print("Creating kafka topic, table, and mv...")
    
    sql_create_table = f"""
    CREATE TABLE {tables['kafka']}
    (
        `repo` String,
        `date` DateTime,
        `stars_gained_that_period` UInt32,
        `prs_opened_that_period` UInt32,
        `cumulative_stars` UInt32,
        `cumulative_prs` UInt32
    )
    ENGINE = Kafka
    SETTINGS 
        kafka_broker_list = 'broker:29092',
        kafka_topic_list = '{tables["topic"]}',
        kafka_group_name = 'clickhouse_github_consumer_{tables["topic"].replace("-", "_")}',
        kafka_format = 'JSONEachRow',
        kafka_num_consumers = 1;
    """
    client.command(sql_create_table)

    sql_materialized_view = f"""
    CREATE MATERIALIZED VIEW {tables['mv']} TO {tables['main']} AS
    SELECT 
        repo,
        date,
        stars_gained_that_period,
        prs_opened_that_period,
        cumulative_stars,
        cumulative_prs
    FROM {tables['kafka']};
    """
    client.command(sql_materialized_view)
    print("Done")

def get_new_record(tables, config):
    """Collect and send new record to Kafka"""
    try:
        # Get recent activity
        activity = get_recent_activity(lookback_hours=config['lookback_hours'])
        
        print(f"In the past {activity['lookback_hours']} hour(s):")
        print(f"New stars: {activity['new_stars']}")
        print(f"New PRs: {activity['new_prs']}")
        print(f"Checked at: {activity['checked_at']}")

        # Get current cumulative values
        result = client.query(f'SELECT cumulative_stars, cumulative_prs FROM {tables["main"]} ORDER BY date DESC LIMIT 1')
        cumulative_stars, cumulative_prs = result.result_rows[0]
        
        # DEBUG: Show what we're working with
        print(f"Current cumulative from DB: stars={cumulative_stars}, prs={cumulative_prs}")
        print(f"Adding: stars={activity['new_stars']}, prs={activity['new_prs']}")
        
        new_cumulative_stars = cumulative_stars + activity['new_stars']
        new_cumulative_prs = cumulative_prs + activity['new_prs']
        
        print(f"Calculated new cumulative: stars={new_cumulative_stars}, prs={new_cumulative_prs}")

        # Create new record with correct field names
        new_record = {
            "repo": "apache/kafka",
            "date": activity['checked_at'],
            "stars_gained_that_period": activity['new_stars'],
            "prs_opened_that_period": activity['new_prs'],
            "cumulative_stars": new_cumulative_stars,
            "cumulative_prs": new_cumulative_prs
        }

        # Send to Kafka
        producer = Producer({'bootstrap.servers': 'localhost:9092'})
        producer.produce(tables['topic'], json.dumps(new_record).encode('utf-8'))
        producer.flush()
        
        print(f"Sent record to {tables['topic']}: {new_record}")
        
        # DEBUG: Wait a moment and check if it was actually inserted
        import time
        time.sleep(2)
        check_result = client.query(f'SELECT cumulative_stars, cumulative_prs FROM {tables["main"]} ORDER BY date DESC LIMIT 1')
        latest_stars, latest_prs = check_result.result_rows[0]
        print(f"After insert, latest in DB: stars={latest_stars}, prs={latest_prs}")
        
    except Exception as e:
        print(f"Error in get_new_record: {e}")

def main():
    """Main execution function"""
    config = GRANULARITY_CONFIG
    tables = setup_tables(config['table_prefix'])
    
    # Setup
    ingest_historic_data(tables)
    create_kafka_infrastructure(tables)
    
    # Send initial record
    print("Sending initial record...")
    get_new_record(tables, config)
    
    # Start continuous collection
    print(f"Starting continuous data collection every {config['interval_minutes']} minutes")
    print(f"Looking back {config['lookback_hours']} hours each time")
    
    interval_seconds = config['interval_minutes'] * 60
    
    while True:
        sleep(interval_seconds)
        Thread(target=lambda: get_new_record(tables, config)).start()

if __name__ == '__main__':
    main()