from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from create_tables import Tables
import pandas as pd
from create_tables import Store
from prefect import task
from dotenv import load_dotenv
load_dotenv()
fake = Faker()


@task(retries=0, persist_result=False)
def generate_fo_trades_trs(store, num_records=1000):
    # Fetch counterparties from ClickHouse
    counterparties = fetch_counterparties_from_clickhouse(store)
    books = fetch_books_from_clickhouse(store)
    underlying_assets = fetch_underlying_assets_from_clickhouse(store)

    data = []
    for _ in range(num_records):
        record = {
            'id': str(uuid.uuid4()),
            'eventId': random.randint(10000, 99999),
            'counterparty': random.choice(counterparties),
            'instrument': random.choice(underlying_assets),
            'book': random.choice(books),
            'tradeDate': fake.date_between(start_date='-1y', end_date='today'),
            'maturityDate': fake.date_between(start_date='today', end_date='+5y'),
            'underlyingAsset': random.choice(underlying_assets),
            'notionalAmount': round(random.uniform(1000000, 100000000), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            # 'payment_frequency': random.choice(['Monthly', 'Quarterly', 'Semi-Annual', 'Annual']),
            'financingSpread': round(random.uniform(0.0001, 0.05), 4),
            'initialPrice': round(random.uniform(10, 1000), 6),
            'collateralType': random.choice(['ABS', 'CLO', 'GOVS','LOAN','CDO','CDS','MBS']),  
            'updatedAt': fake.date_time_between(start_date='-1y', end_date='now'),
        }
        data.append(record)
    return data 

@task(retries=0, persist_result=False)
def fetch_counterparties_from_clickhouse(store):
    
    # Execute the query to fetch counterparties
    result = store.client.query("SELECT DISTINCT id FROM ref_counterparties")

    # Extract counterparty IDs from the result
    counterparties = [row[0] for row in result.result_rows]

    return counterparties


@task(retries=0, persist_result=False)
def fetch_books_from_clickhouse(store):
    # Execute the query to fetch books
    result = store.client.query("SELECT DISTINCT book FROM ref_hms")

    # Extract book IDs from the result
    books = [row[0] for row in result.result_rows]

    return books

@task(retries=0, persist_result=False)
def fetch_underlying_assets_from_clickhouse(store):
    # Execute the query to fetch underlying assets
    result = store.client.query("SELECT DISTINCT id FROM ref_instruments")

    # Extract underlying asset IDs from the result
    underlying_assets = [row[0] for row in result.result_rows]

    return underlying_assets    

@task(retries=0, persist_result=False)
def load_trades_to_clickhouse(store, data):
    
    df = pd.DataFrame(data)
    df.set_index('id', inplace=True)
    store.client.insert_df(Tables.TRADES.value, df.reset_index())


if __name__ == "__main__":
    
    import os
    print(os.getenv("PREFECT_API_URL"))
    store = Store()
    data = generate_fo_trades_trs(store, num_records=15)
    print(data)
    load_trades_to_clickhouse(store, data)