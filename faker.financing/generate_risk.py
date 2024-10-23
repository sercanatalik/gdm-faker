import pandas as pd
import uuid,time
import random
from datetime import datetime
from clickhouse_connect import get_client
from decimal import Decimal
import clickhouse_connect

client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='default', password='')


def generate_fo_risk_data(client):
    # Fetch trade data to base risk data on
    trades = client.query("SELECT * FROM trades")
    snapId = 'LIVE'+datetime.now().strftime("%Y%m%d")

    risk_data = []
    for trade in trades.named_results():
        risk_record = {
            'id': trade['id'],
            'tradeId': trade['id'],
            'notionalAmount': Decimal(str(trade['notionalAmount'])),  # Use the original notional amount
            'cashout': Decimal(str(random.uniform(0, float(trade['notionalAmount']) * random.uniform(0.5, 1)))),
            'spread': Decimal(str(trade['financingSpread'])),
            'accrualDaily': Decimal(str(float(trade['financingSpread']) * float(trade['notionalAmount']) / 365 * random.uniform(0.5, 2.0))),
            'accrualPast': Decimal(str(float(trade['financingSpread']) * float(trade['notionalAmount']) * 90 / 365 * random.uniform(0.5, 2.0))),
            'accrualProjected': Decimal(str(float(trade['financingSpread']) * float(trade['notionalAmount']) * 150 / 365 * random.uniform(0.5, 2.0))),
            'ead': Decimal(str(float(trade['notionalAmount']) * 0.4)), 
            'fxSpot': Decimal(str(random.uniform(0.5, 2.0))),
            'ccy': trade['currency'],
            'updatedAt': datetime.now(),
            'snapId': snapId,
            'book': trade['book'],
            'asOfDate': datetime.today(),
            'instrument': trade['instrument'],
            'counterparty': trade['counterparty'],
            'version': client.query(f"SELECT MAX(version) FROM risk WHERE tradeId = '{trade['id']}'").first_row[0] + 1
        }
        risk_data.append(risk_record)
    
    return risk_data

def insert_fo_risk_data(client, risk_data):
    df = pd.DataFrame(risk_data)
    client.insert_df("risk", df)




if __name__ == "__main__":
    
    risk_data = generate_fo_risk_data(client)
    insert_fo_risk_data(client, risk_data)


    while True:
        risk_data = generate_fo_risk_data(client)
        insert_fo_risk_data(client, risk_data)
        print(f"Inserted {len(risk_data)} risk records",datetime.now())
        time.sleep(5)