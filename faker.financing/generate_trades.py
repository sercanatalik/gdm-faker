
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import clickhouse_connect
from create_tables import Tables
import pandas as pd
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123)

fake = Faker()



def generate_fo_trades_trs(client, num_records=1000):
    # Fetch counterparties from ClickHouse
    counterparties = fetch_counterparties_from_clickhouse(client)
    books = fetch_books_from_clickhouse(client)
    underlying_assets = fetch_underlying_assets_from_clickhouse(client)

    data = []
    for _ in range(num_records):
        record = {
            'id': str(uuid.uuid4()),
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


def fetch_counterparties_from_clickhouse(client):
    
    # Execute the query to fetch counterparties
    result = client.query("SELECT DISTINCT name FROM ref_counterparties")

    # Extract counterparty IDs from the result
    counterparties = [row[0] for row in result.result_rows]

    return counterparties


def fetch_books_from_clickhouse(client):
    # Execute the query to fetch books
    result = client.query("SELECT DISTINCT book FROM ref_hms")

    # Extract book IDs from the result
    books = [row[0] for row in result.result_rows]

    return books

def fetch_underlying_assets_from_clickhouse(client):
    # Execute the query to fetch underlying assets
    result = client.query("SELECT DISTINCT id FROM ref_instruments")

    # Extract underlying asset IDs from the result
    underlying_assets = [row[0] for row in result.result_rows]

    return underlying_assets    

def load_trades_to_clickhouse(client, data):
    
    df = pd.DataFrame(data)
    df.set_index('id', inplace=True)
    client.insert_df(Tables.TRADES.value, df.reset_index())


if __name__ == "__main__":

    data = generate_fo_trades_trs(client, num_records=1)
    load_trades_to_clickhouse(client, data)