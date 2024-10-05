import uuid
import random
from faker import Faker
from clickhouse_connect import get_client

fake = Faker()


def create_fo_trades_trs_table(table_name):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        trade_id String,
        counterparty String,
        book String,
        trade_date Date,
        effective_date Date,
        maturity_date Date,
        underlying_asset String,
        notional_amount Decimal(18,2),
        currency String,
        payment_frequency Enum8('Monthly' = 1, 'Quarterly' = 2, 'Semi-Annual' = 3, 'Annual' = 4),
        total_return_receiver String,
        total_return_payer String,
        financing_rate String,
        financing_spread Decimal(5,4),
        initial_price Decimal(18,6),
        is_cleared UInt8,
        clearing_house String,
        collateral_type Enum8('Cash' = 1, 'Securities' = 2, 'Both' = 3),
        termination_date Date,
        termination_price Decimal(18,6),
        status Enum8('Active' = 1, 'Matured' = 2, 'Terminated' = 3),
        created_at DateTime,
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,trade_id);
    """

    return create_table_sql.format(table_name=table_name)


def generate_fo_trades_trs(client, num_records=1000):
    # Fetch counterparties from ClickHouse
    counterparties = fetch_counterparties_from_clickhouse(client)
    books = fetch_books_from_clickhouse(client)
    underlying_assets = fetch_underlying_assets_from_clickhouse(client)

    
    data = []
    for i in range(num_records):
        record = {
            'id': str(uuid.uuid4()),
            'trade_id': str(uuid.uuid4()),
            'book': random.choice(books),
            'counterparty': random.choice(counterparties),
            'trade_date': fake.date_between(start_date='-1y', end_date='today'),
            'effective_date': fake.date_between(start_date='-1y', end_date='today'),
            'maturity_date': fake.date_between(start_date='-1y', end_date='today'),
            'underlying_asset': random.choice(underlying_assets),
            'notional_amount': round(random.uniform(1000, 100000), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            'payment_frequency': random.choice(['Monthly', 'Quarterly', 'Semi-Annual', 'Annual']),
            'total_return_receiver': str(uuid.uuid4()),
            'total_return_payer': str(uuid.uuid4()),
            'financing_rate': str(uuid.uuid4()),
            'financing_spread': round(random.uniform(0.0001, 0.05), 4),
            'initial_price': round(random.uniform(1000, 100000), 2),
            'is_cleared': random.choice([0, 1]),
            'clearing_house': str(uuid.uuid4()),
            'collateral_type': random.choice(['Cash', 'Securities', 'Both']),
            'termination_date': fake.date_between(start_date='-1y', end_date='today'),
            'termination_price': round(random.uniform(1000, 100000), 2),
            'status': random.choice(['Active', 'Matured', 'Terminated']),
            'created_at': fake.date_time_between(start_date='-1y', end_date='now'),
            'updated_at': fake.date_time_between(start_date='-1y', end_date='now'),
        }
        data.append(record)
    return data


def fetch_counterparties_from_clickhouse(client):
    
    # Execute the query to fetch counterparties
    result = client.query("SELECT DISTINCT name FROM fo_counterparty")

    # Extract counterparty IDs from the result
    counterparties = [row[0] for row in result.result_rows]

    return counterparties


def fetch_books_from_clickhouse(client):
    # Execute the query to fetch books
    result = client.query("SELECT DISTINCT book FROM fo_hms")

    # Extract book IDs from the result
    books = [row[0] for row in result.result_rows]

    return books

def fetch_underlying_assets_from_clickhouse(client):
    # Execute the query to fetch underlying assets
    result = client.query("SELECT DISTINCT id FROM fo_instrument")

    # Extract underlying asset IDs from the result
    underlying_assets = [row[0] for row in result.result_rows]

    return underlying_assets    
