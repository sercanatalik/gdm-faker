import pandas as pd
import uuid,time
import random
from datetime import datetime
from clickhouse_connect import get_client
from decimal import Decimal
from clickhouse_connect.driver.exceptions import ClickHouseError

def create_fo_risk_table(client):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fo_risk (
        id String,
        tradeId String,
        notional Decimal(18,2),
        cashout Decimal(18,2),
        spread Decimal(5,4),
        accrual_daily Decimal(18,2),
        accrual_past Decimal(18,2),
        accrual_projected Decimal(18,2),
        ead Decimal(18,2),
        fxSpot Decimal(10,6),
        ccy String,
        updated_at DateTime,
        snapId String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (tradeId, snapId);
    """

    client.command(create_table_sql)

def generate_fo_risk_data(client, snapId:str = 'LIVE'+datetime.now().strftime("%Y%m%d")):
    # Fetch trade data to base risk data on
    trades = client.query("SELECT id, notional_amount, financing_spread, currency FROM fo_trades_trs")
    

    risk_data = []
    for trade in trades.named_results():
        risk_record = {
            'id': trade['id'],
            'tradeId': trade['id'],
            'notional': Decimal(str(trade['notional_amount'])),  # Use the original notional amount
            'cashout': Decimal(str(random.uniform(0, random.uniform(1,0.5)*float(trade['notional_amount'])))),
            'spread': Decimal(str(trade['financing_spread'])),
            'accrual_daily': Decimal(str(random.uniform(0, 1000))),
            'accrual_past': Decimal(str(random.uniform(0, 10000))),
            'accrual_projected': Decimal(str(random.uniform(0, 50000))),
            'ead': Decimal(str(trade['notional_amount'])),  # Simplified EAD calculation
            'fxSpot': Decimal(str(random.uniform(0.5, 2.0))),
            'ccy': trade['currency'],
            'updated_at': datetime.now(),
            'snapId': snapId,
            
          
        }
        risk_data.append(risk_record)
    
    return risk_data

def insert_fo_risk_data(client, risk_data):
    df = pd.DataFrame(risk_data)
    client.insert_df("fo_risk", df)

def create_risk_materialized(client):
    # Drop the existing view if it exists
    drop_sql = "DROP TABLE IF EXISTS mv_fo_financing_trades"
    client.command(drop_sql)

    # Create the new materialized view
    create_sql = """
    CREATE MATERIALIZED VIEW fo_risk_mv TO fo_risk_raw

    ORDER BY (t.id)
    
    AS SELECT
        t.*,
        h.*,
        r.*      
    FROM
        fo_trades_trs AS t
    JOIN fo_hms h ON t.book = h.book
    JOIN fo_risk r ON t.id = r.id
    """
    client.command(create_sql)


def stats_risk_materialized(client):
    query = "SELECT sum(accrual_daily),sum(accrual_past),sum(accrual_projected),sum(ead) FROM mv_fo_financing_trades"
    result = client.query(query)
    time.sleep(1)
    result_dict = dict(zip(result.column_names, result.result_rows[0]))
    print(result_dict)

def create_materilized_financing_stats(client):
    # Drop the existing view if it exists
    drop_sql = "DROP TABLE IF EXISTS mv_fo_stats"
    client.command(drop_sql)
    print("Dropped mv_fo_stats")
    # Create the new materialized view
    create_sql = """
    CREATE MATERIALIZED VIEW mv_fo_stats
    ENGINE = ReplacingMergeTree()
    ORDER BY (book, snapId)
    POPULATE
    AS SELECT
    
        h.book as book,
        h.desk as desk,
        r.snapId as snapId,
        sum(r.accrual_daily) as total_accrual_daily,
        sum(r.accrual_projected) as total_accrual_projected,
        sum(r.cashout) as total_cashout,
        sum(r.notional) as total_notional,
        sum(r.ead) as total_ead,
        max(r.updated_at) as updated_at
    FROM
        fo_trades_trs AS t
    JOIN fo_hms h ON t.book = h.book
    JOIN fo_risk r ON t.id = r.id
    GROUP BY h.book, h.desk, r.snapId
    """
    client.command(create_sql)

def get_accrual_by_book(client, snapId):
    query = f"""
    SELECT book, total_accrual_daily
    FROM mv_fo_stats
    WHERE snapId = '{snapId}'
    """
    result = client.query(query)
    return result.named_results()

def main():
    client = get_client(host='127.0.0.1', port=8123, username='default', password='')
    create_fo_risk_table(client)
    risk_data = generate_fo_risk_data(client,snapId='LIVE'+datetime.now().strftime("%Y%m%d"))
    insert_fo_risk_data(client, risk_data)
    create_risk_materialized(client)
    create_materilized_financing_stats(client)

    loop_generate_risk_data(client, interval=10)

# Add a function to query the new materialized view
def generate_risk_data(client):
    snapId = 'LIVE'+datetime.now().strftime("%Y%m%d")
    risk_data = generate_fo_risk_data(client, snapId)
    insert_fo_risk_data(client, risk_data)
   
def loop_generate_risk_data(client, interval=10):
    while True:
        generate_risk_data(client)
        print("Generated risk data",datetime.now())
        time.sleep(interval)


if __name__ == "__main__":
    main()