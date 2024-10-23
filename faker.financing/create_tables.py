import clickhouse_connect
import asyncio
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123)

import enum

class Tables(enum.Enum):
    HMSBOOKS = "ref_hms"
    DBNAME = "default"
    COUNTERPARTIES = "ref_counterparties"
    INSTRUMENTS = "ref_instruments"
    TRADES = "trades"
    RISK = "risk"
    RISKVIEW = "risk_view"
    RISKVIEW_MV = "risk_view_mv"
    RISK_AGGREGATING_VIEW = "risk_agg"
    RISK_AGGREGATING_VIEW_MV = "risk_agg_mv"
    

async def create_db():
    query = f"""
    DROP DATABASE IF EXISTS {Tables.DBNAME.value}"""
    client.command(query)
    query = f"""
    CREATE DATABASE IF NOT EXISTS {Tables.DBNAME.value}"""
    client.command(query)
    
async def create_hms_tables():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.HMSBOOKS.value} (
        id String,
        book String,
        trader LowCardinality(String),
        desk LowCardinality(String),
        updatedAt DateTime,
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,book,updatedAt);
    """
    client.command(query)


async def  create_counterparty_tables():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.COUNTERPARTIES.value} (
        id String,
        name String,
        country LowCardinality(String),
        sector LowCardinality(String),
        industry LowCardinality(String),
        rating LowCardinality(String),
        updatedAt DateTime,
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,name,updatedAt);
    """
    client.command(query)


async def create_instrument_tables():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.INSTRUMENTS.value} (
    id String,
        isin String,
        cusip String,
        sedol String,
        name String,
        issuer String,
        region LowCardinality(String),
        country LowCardinality(String),
        sector LowCardinality(String),
        industry LowCardinality(String),
        currency LowCardinality(String),
        issueDate Date,
        maturityDate Date,
        coupon Decimal(5,2),
        couponFrequency LowCardinality(String),
        yieldToMaturity Decimal(5,2),
        price Decimal(10,2),
        faceValue Decimal(10,2),
        rating LowCardinality(String),
        isCallable UInt8,
        isPuttable UInt8,
        isConvertible UInt8,
        updatedAt DateTime
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,updatedAt);
    """
    client.command(query)


async def create_trades_tables():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.TRADES.value} (
        id String,
        counterparty LowCardinality(String),
        instrument LowCardinality(String),
        book LowCardinality(String),
        tradeDate Date,
        maturityDate Date,
        underlyingAsset LowCardinality(String),
        notionalAmount Decimal(18,2),
        currency LowCardinality(String),
        financingSpread Decimal(5,4),
        initialPrice Decimal(18,6),
        collateralType LowCardinality(String),  
        updatedAt DateTime

    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id,updatedAt);
    """
    client.command(query)

async def create_risk_tables():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISK.value} (
        id String,
        tradeId String,
        version UInt64,
        book LowCardinality(String),
        notionalAmount Decimal(18,2),
        instrument LowCardinality(String),
        counterparty LowCardinality(String),
        cashout Decimal(18,2),
        spread Decimal(5,4),
        accrualDaily Decimal(18,2),
        accrualPast Decimal(18,2),
        accrualProjected Decimal(18,2),
        ead Decimal(18,2),
        fxSpot Decimal(10,6),
        ccy LowCardinality(String),
        updatedAt DateTime,
        snapId LowCardinality(String),
        asOfDate Date
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (id, asOfDate)
    PRIMARY KEY (id)
    SETTINGS index_granularity = 8192;
    """
    client.command(query)   


async def create_risk_view():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISKVIEW.value} (
        id String,
        tradeId String,
        version UInt64,
        book LowCardinality(String),
        notionalAmount Decimal(18,2),
        instrument LowCardinality(String),
        counterparty LowCardinality(String),
        updatedAt DateTime,
        snapId LowCardinality(String),
        asOfDate Date,
        ccy LowCardinality(String),
        cpSector LowCardinality(String),
        cpIndustry LowCardinality(String),
        cpRating LowCardinality(String),
        hmsBook LowCardinality(String),
        hmsTrader LowCardinality(String),
        hmsDesk LowCardinality(String),
        dailyAccrual Decimal(18,2),
        pastAccrual Decimal(18,2),
        projectedAccrual Decimal(18,2),
        cashout Decimal(18,2),
        ead Decimal(18,2),
        fxSpot Decimal(10,6),
        spread Decimal(5,4),
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (id,asOfDate)
    """
    client.command(query)

async def create_risk_view_mv():
    query = f"""
    CREATE MATERIALIZED VIEW {Tables.RISKVIEW_MV.value} TO {Tables.RISKVIEW.value}
    AS SELECT
    r.id as id,
    r.tradeId as tradeId,
    r.version as version,
    r.book as book,
    r.instrument as instrument,
    r.counterparty as counterparty,
    r.updatedAt as updatedAt,
    r.snapId as snapId,
    r.notionalAmount as notionalAmount,
    r.asOfDate as asOfDate,
    r.cashout as cashout,
    r.ccy as ccy,
    cp.sector AS cpSector,
    cp.industry AS cpIndustry,
    cp.rating AS cpRating,
    hms.book AS hmsBook,
    hms.trader AS hmsTrader,
    hms.desk AS hmsDesk,
    r.accrualDaily as dailyAccrual,
    r.accrualPast as pastAccrual,
    r.accrualProjected as projectedAccrual,
    r.ead as ead,
    r.fxSpot as fxSpot,
    r.spread as spread
    FROM {Tables.RISK.value} r
    INNER JOIN {Tables.COUNTERPARTIES.value} cp ON r.counterparty = cp.name
    INNER JOIN {Tables.HMSBOOKS.value} hms ON r.book = hms.book
    """
    client.command(query)



async def create_risk_aggregating_view():
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISK_AGGREGATING_VIEW.value} (
        hmsDesk LowCardinality(String),
        hmsTrader LowCardinality(String),
        book LowCardinality(String),
        asOfDate Date,
        totalNotionalAmount Decimal(38,2),
        totalDailyAccrual Decimal(38,2),
        totalCashout Decimal(38,2),
        totalEad Decimal(38,2),
        totalProjectedAccrual Decimal(38,2),
        totalPastAccrual Decimal(38,2),
        version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (hmsDesk, hmsTrader, book, asOfDate);
    """
    client.command(query)

    # Update the materialized view query
    mv_query = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {Tables.RISK_AGGREGATING_VIEW_MV.value} TO {Tables.RISK_AGGREGATING_VIEW.value}
    AS SELECT
        hmsDesk,
        hmsTrader,
        book,
        asOfDate,
        sum(notionalAmount) AS totalNotionalAmount,
        sum(dailyAccrual) AS totalDailyAccrual,
        sum(cashout) AS totalCashout,
        sum(ead) AS totalEad,
        sum(projectedAccrual) AS totalProjectedAccrual,
        sum(pastAccrual) AS totalPastAccrual,
        max(version) AS version
    FROM {Tables.RISKVIEW.value}
    GROUP BY hmsDesk, hmsTrader, book, asOfDate;
    """
    client.command(mv_query)







async def main():
    await create_db()
    await create_hms_tables()   
    await create_counterparty_tables()
    await create_instrument_tables()
    await create_trades_tables()
    await create_risk_tables()
    await create_risk_view()
    await create_risk_view_mv()
    await create_risk_aggregating_view()
    # await create_risk_materialized_view()

if __name__ == "__main__":
    asyncio.run(main())    




