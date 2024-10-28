
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import clickhouse_connect
from create_tables import Tables
import pandas as pd
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123)

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




def generate_fo_hms_data(num_records=25):
    data = []
    for i in range(num_records):
        record = {
            'trader': random.choice(traders),
            'desk': random.choice(desks),
            'book': random.choice(books),
            'id': fake.uuid4(),
            'updatedAt': datetime.now()
        }

        data.append(record)
    return data


def load_hms_data():
    hms_data = generate_fo_hms_data(num_records=100)
    df = pd.DataFrame(hms_data)
    df.set_index('id', inplace=True)
    client.insert_df(Tables.HMSBOOKS.value, df.reset_index())





def generate_fo_counterparty_data(num_records=1000):
    data = []
    for i in range(num_records):
        record = {
            'id': str(uuid.uuid4()),
            'name': fake.company(),
            'country': random.choice(countries),
            'sector': random.choice(['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer Goods']),
            'industry': fake.job(),
            'rating': random.choice(['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']),
            'updatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        }
        data.append(record)
    return data          


def load_counterparty_data():
    counterparty_data = generate_fo_counterparty_data(num_records=1000)
    df = pd.DataFrame(counterparty_data)
    df.set_index('id', inplace=True)
    client.insert_df(Tables.COUNTERPARTIES.value, df.reset_index())



def generate_random_isin():
    country_code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    security_identifier = ''.join(random.choices('0123456789', k=9))
    check_digit = random.randint(0, 9)
    return f"{country_code}{security_identifier}{check_digit}"


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
            'issueDate': issue_date,
            'maturityDate': maturity_date,
            'coupon': round(random.uniform(0, 10), 2),
            'couponFrequency': random.choice(['Annual', 'Semi-Annual', 'Quarterly']),
            'yieldToMaturity': round(random.uniform(0, 15), 2),
            'price': round(random.uniform(50, 150), 2),
            'faceValue': round(random.uniform(500, 2000), 2),
            'rating': random.choice(['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']),
            'isCallable': random.choice([0, 1]),
            'isPuttable': random.choice([0, 1]),
            'isConvertible': random.choice([0, 1]),
            'updatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        }
        data.append(record)
    return data

def load_instrument_data():
    instrument_data = generate_fo_instrument_data(num_records=1000)
    df = pd.DataFrame(instrument_data)
    df.set_index('id', inplace=True)
    client.insert_df(Tables.INSTRUMENTS.value, df.reset_index())
    





if __name__ == "__main__":
    load_hms_data()
    load_counterparty_data()
    load_instrument_data()

