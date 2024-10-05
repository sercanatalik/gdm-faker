import pandas as pd
import uuid
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
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (tradeId, updated_at);
    """

    client.command(create_table_sql)

def generate_fo_risk_data(client, num_records=1000):
    # Fetch trade data to base risk data on
    trades = client.query("SELECT id, trade_id, notional_amount, financing_spread, currency FROM fo_trades_trs LIMIT {}".format(num_records))
    
    risk_data = []
    for trade in trades.named_results():
        risk_record = {
            'id': trade['trade_id'],
            'tradeId': trade['trade_id'],
            'notional': Decimal(str(trade['notional_amount'])),
            'cashout': Decimal(str(random.uniform(0, float(trade['notional_amount'])))),
            'spread': Decimal(str(trade['financing_spread'])),
            'accrual_daily': Decimal(str(random.uniform(0, 1000))),
            'accrual_past': Decimal(str(random.uniform(0, 10000))),
            'accrual_projected': Decimal(str(random.uniform(0, 50000))),
            'ead': Decimal(str(trade['notional_amount'])),  # Simplified EAD calculation
            'fxSpot': Decimal(str(random.uniform(0.5, 2.0))),
            'ccy': trade['currency'],
            'updated_at': datetime.now()
        }
        risk_data.append(risk_record)
    
    return risk_data

def insert_fo_risk_data(client, risk_data):
    df = pd.DataFrame(risk_data)
    client.insert_df("fo_risk", df)

def create_risk_materialized(client):
    # Drop the existing view if it exists
    drop_sql = "DROP TABLE IF EXISTS mv_fo_financing_risk"
    client.command(drop_sql)

    # Create the new materialized view
    create_sql = """
    CREATE MATERIALIZED VIEW mv_fo_financing_risk
    ENGINE = MergeTree()
    ORDER BY (t.id)
    POPULATE
    AS SELECT
        t.*,
        h.*,
        c.*,
        i.*
    FROM
        fo_trades_trs t
    JOIN fo_hms h ON t.book = h.book
    JOIN fo_counterparty c ON t.counterparty = c.name
    JOIN fo_instrument i ON t.underlying_asset = i.isin
    """
    client.command(create_sql)

def main():
    client = get_client(host='127.0.0.1', port=8123, username='default', password='')
    create_fo_risk_table(client)
    risk_data = generate_fo_risk_data(client)
    insert_fo_risk_data(client, risk_data)
    create_risk_materialized(client)


if __name__ == "__main__":
    main()


