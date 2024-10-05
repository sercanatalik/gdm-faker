import faker
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import clickhouse_connect

def generate_fake_exposure_data(num_records=100):
    fake = faker.Faker()
    
    data = []
    used_trade_ids = set()
    for _ in range(num_records):
        while True:
            trade_id = fake.unique.random_number(digits=9)
            if trade_id not in used_trade_ids:
                used_trade_ids.add(trade_id)
                break

        institution_type = fake.random_element(elements=('Hedge Fund', 'Bank'))
        institution_name = fake.company()
        exposure_amount = round(fake.random.uniform(1000000, 1000000000), 2)
        exposure_date = fake.date_between(start_date='-1y', end_date='today')
        risk_rating = fake.random_int(min=1, max=10)
        
        cash_out = round(fake.random.uniform(0, exposure_amount), 2)
        notional = round(fake.random.uniform(exposure_amount, exposure_amount * 1.5), 2)
        underlying_asset = fake.random_element(elements=('Stocks', 'Bonds', 'Commodities', 'Derivatives'))
        underlying_rating = fake.random_element(elements=('AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC'))
        underlying_sector = fake.random_element(elements=('Technology', 'Finance', 'Healthcare', 'Energy', 'Consumer Goods'))
        maturity = fake.date_between(start_date='today', end_date='+5y')
        
        data.append({
            'tradeId': trade_id,
            'institution_type': institution_type,
            'institution_name': institution_name,
            'risk_rating': risk_rating,
            'cash_out': cash_out,
            'notional': notional,
            'underlying_asset': underlying_asset,
            'underlying_rating': underlying_rating,
            'underlying_sector': underlying_sector,
            'maturity': maturity
        })
    
    df = pd.DataFrame(data)
    df.set_index('tradeId', inplace=True)
    return df

def save_to_clickhouse(df):
    client = clickhouse_connect.get_client(host='127.0.0.1', port=8123)
    
    try:
        client.command('DROP TABLE IF EXISTS default.gdm_financing_risk')
        client.command('''
            CREATE TABLE default.gdm_financing_risk (
                tradeId UInt32,
                institution_type String,
                institution_name String,
                risk_rating UInt8,
                cash_out Float64,
                notional Float64,
                underlying_asset String,
                underlying_rating String,
                underlying_sector String,
                maturity Date
            ) ENGINE = MergeTree()
            ORDER BY tradeId
        ''')
        # Insert the data directly from pandas DataFrame
        client.insert_df('default.gdm_financing_risk', df.reset_index())
        print("Data saved to ClickHouse table 'default.gdm_financing_risk'")
    
    finally:
        # Close the client connection
        client.close()

async def main():
    df = generate_fake_exposure_data(1000)
    print(df.head())
    
    # Save to ClickHouse
    await asyncio.to_thread(save_to_clickhouse, df)

if __name__ == "__main__":
    asyncio.run(main())
