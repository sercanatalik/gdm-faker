from faker import Faker
from datetime import datetime, timedelta
import random
import uuid

fake = Faker()

traders = ['John Smith', 'Emma Johnson', 'Michael Brown', 'Sarah Davis', 'Robert Wilson']
desks = ['Flow Credit', 'Structured Index', 'Prime Broker', 'Private Credit', 'Commodities']
portfolios = [
    'Global Equity',
    'Fixed Income Arbitrage',
    'Emerging Markets Debt',
    'Quantitative Strategies',
    'Distressed Securities',
    'Long/Short Equity',
    'Convertible Arbitrage',
    'Macro Trading',
    'Event-Driven',
    'Merger Arbitrage',
    'High-Yield Bonds',
    'Sovereign Debt',
    'Real Estate Securities',
    'Infrastructure Finance',
    'Commodity Trading',
    'Volatility Arbitrage',
    'Credit Default Swaps',
    'Structured Products',
    'Leveraged Loans',
    'Algorithmic Trading',
    'Private Equity',
    'Venture Capital',
    'Mezzanine Financing',
    'Green Energy Finance',
    'Blockchain Assets'
]
books = [f"{fake.lexify('????').upper()}{fake.numerify('###')}" for _ in range(100)]
regions = ['North America', 'Europe', 'Asia', 'Latin America', 'Africa', 'Australia']
balance_sheet = ['HBEU','HBUS','HBAP','HBCE']
countries = ['United States', 'United Kingdom', 'Germany', 'France', 'Japan', 'China', 'India', 'Brazil', 'Russia', 'South Africa']



def generate_random_isin():
    country_code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    security_identifier = ''.join(random.choices('0123456789', k=9))
    check_digit = random.randint(0, 9)
    return f"{country_code}{security_identifier}{check_digit}"

def create_fo_hms_table(table_name):
    
    # SQL statement to create the fo_hms table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        balance_sheet String,
        trader String,
        desk String,
        portfolio String,
        book String,
        region String,
        bookGuid String,
        updatedAt DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,bookGuid,updatedAt);
    """

    return create_table_sql.format(table_name=table_name)

def create_fo_counterparty_table(table_name):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        name String,
        region String,
        country String,
        sector String,
        industry String,
        rating String,
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,name,updated_at);
    """

    return create_table_sql.format(table_name=table_name)


def create_fo_instrument_table(table_name):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        isin String,
        cusip String,
        sedol String,
        name String,
        issuer String,
        region String,
        country String,
        sector String,
        industry String,
        currency String,
        issue_date Date,
        maturity_date Date,
        coupon Decimal(5,2),
        coupon_frequency String,
        yield_to_maturity Decimal(5,2),
        price Decimal(10,2),
        face_value Decimal(10,2),
        rating String,
        is_callable UInt8,
        is_puttable UInt8,
        is_convertible UInt8,
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,updated_at);
    """

    return create_table_sql.format(table_name=table_name)



def generate_fo_hms_data(num_records=25):
    data = []
    for i in range(num_records):
        record = {
            'balance_sheet': random.choice(balance_sheet),
            'trader': random.choice(traders),
            'desk': random.choice(desks),
            'portfolio': random.choice(portfolios),
            'book': random.choice(books),
            'region': random.choice(regions),
            'bookGuid': fake.uuid4(),
            'updatedAt': datetime.now()
        }
        record['id'] = record['bookGuid']   

        data.append(record)
    return data

def generate_fo_instrument_data(num_records=1000):
    data = []

    names = [f"{random.choice(countries)}" for _ in range(25)]
    names.extend([f"{fake.company()}" for _ in range(50)])


    for i in range(num_records):
        issue_date = fake.date_between(start_date='-10y', end_date='today')
        maturity_date = issue_date + timedelta(days=random.randint(365, 3650))
        id = generate_random_isin()
        name = random.choice(names)

        record = {
            'id': id,
            'isin': id,
            'cusip': ''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=9)),
            'sedol': ''.join(random.choices('0123456789', k=7)),
            'name': name,
            'issuer': name,
            'region': random.choice(regions),
            'country': random.choice(countries),
            'sector': random.choice(['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer Goods']),
            'industry': fake.job(),
            'currency': fake.currency_code(),
            'issue_date': issue_date,
            'maturity_date': maturity_date,
            'coupon': round(random.uniform(0, 10), 2),
            'coupon_frequency': random.choice(['Annual', 'Semi-Annual', 'Quarterly']),
            'yield_to_maturity': round(random.uniform(0, 15), 2),
            'price': round(random.uniform(50, 150), 2),
            'face_value': round(random.uniform(500, 2000), 2),
            'rating': random.choice(['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']),
            'is_callable': random.choice([0, 1]),
            'is_puttable': random.choice([0, 1]),
            'is_convertible': random.choice([0, 1]),
            'updated_at': fake.date_time_between(start_date='-1y', end_date='now')
        }
        data.append(record)
    return data

def generate_fo_counterparty_data(num_records=1000):
    data = []
    for i in range(num_records):
        record = {
            'id': str(uuid.uuid4()),
            'name': fake.company(),
            'region': random.choice(regions),
            'country': random.choice(countries),
            'sector': random.choice(['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer Goods']),
            'industry': fake.job(),
            'rating': random.choice(['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']),
            'updated_at': fake.date_time_between(start_date='-1y', end_date='now')
        }
        data.append(record)
    return data          
