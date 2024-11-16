import clickhouse_connect
from prefect import task
import enum

class Store:
    def __init__(self):
        self.client = clickhouse_connect.get_client(host="127.0.0.1", port=8123)
    def close(self):
        self.client.close()


class Tables(enum.Enum):
    HMSBOOKS = "ref_hms"
    DBNAME = "default"
    COUNTERPARTIES = "ref_counterparties"
    INSTRUMENTS = "ref_instruments"
    TRADES = "trades_trs"
    RISK = "risk_f"
    RISKVIEW = "risk_view"
    RISKVIEW_MV = "risk_view_mv"
    RISK_AGGREGATING_VIEW = "risk_agg"
    RISK_AGGREGATING_VIEW_MV = "risk_agg_mv"
    OVERRIDES = "overrides"
    JOBS = "jobs"

@task(retries=0, cache_key_fn=None,persist_result=False)
def create_db(store: Store) -> None:
    """Create the database if it doesn't exist."""
    print(f"Creating {Tables.DBNAME.value} database")
    query = f"""
    DROP DATABASE IF EXISTS {Tables.DBNAME.value}"""
    store.client.command(query)
    query = f"""
    CREATE DATABASE IF NOT EXISTS {Tables.DBNAME.value}"""
    store.client.command(query)


@task(retries=0, cache_key_fn=None, persist_result=False)
def create_hms_tables(store: Store):
    print(f"Creating {Tables.HMSBOOKS.value} table")
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
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_counterparty_tables(store: Store):
    print(f"Creating {Tables.COUNTERPARTIES.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.COUNTERPARTIES.value} ( 
    site String,                    -- 3 character site code (e.g., 'LDN')
    treat4Parent String,            -- 4 character treatment code (e.g., 'ABSA')
    treat7 String,                 -- 7 character treatment code (e.g., 'ABSAJOS')
    countryOfIncorporation String,  -- 2 character ISO country code (e.g., 'ZA')
    countryOfPrimaryOperation String, -- 2 character ISO country code
    customerName String,            -- Full customer name
    lei String,                     -- Legal Entity Identifier (20 character)
    ptsShorName String,            -- Short name identifier
    riskRatingCrr String,          -- Risk rating (e.g., '4.3')
    riskRatingBucket String,       -- Risk bucket (e.g., 'Ba3')
    riskRatingGDP String,          -- GDP risk rating (e.g., 'BB-')
    masterGroup String,            -- Master group name
    cbSector String,               -- Business sector (e.g., 'Banks', 'Hedge Fund')
    id String                      -- Unique identifier
)
ENGINE = ReplacingMergeTree()
ORDER BY id;
    """
    store.client.command(query)


@task(retries=0, cache_key_fn=None, persist_result=False)
def create_instruments_tables(store: Store):
    print(f"Creating {Tables.INSTRUMENTS.value} table")
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
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_trades_tables(store: Store):
    print(f"Creating {Tables.TRADES.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.TRADES.value} (
        id String,
        eventId Int64,
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
    store.client.command(query)


@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_tables(store: Store):
    print(f"Creating {Tables.RISK.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISK.value} (
           id String,
    snapId String,
    eventId Int64,
    snapVersion Int64,
    asOfDate Date,
    status LowCardinality(String),
    book LowCardinality(String),
    tradeDt Date,
    settlementDt Date,
    maturityDt Date,
    notionalCcy Decimal(18,2),
    notionalAmount Decimal(18,2),
    firstReset Decimal(18,2),
    subType LowCardinality(String),
    productType LowCardinality(String),
    ccy LowCardinality(String),
    haircutManual Decimal(18,2),
    bondcfFactor Decimal(18,2),
    iaimAmount Decimal(18,2),
    iaimCcy LowCardinality(String),
    side LowCardinality(String),
    model LowCardinality(String),
    counterparty LowCardinality(String),
    notionalFundingCcy Decimal(18,2),
    marginOis Decimal(18,2),
    marginFixed Decimal(18,2),
    marginFloat Decimal(18,2),
    instrumentId String,
    dtm Int64,
    tenor LowCardinality(String),
    mid Decimal(18,2),
    fxSpot Decimal(18,2),
    sideFactor LowCardinality(String),
    notional Decimal(18,2),
    ccyFunding LowCardinality(String),
    fxspotFunding Decimal(18,2),
    notionalFunding Decimal(18,2),
    iaAmount Decimal(18,2),
    cashOut Decimal(18,2),
    haircut Decimal(18,2),
    margin Decimal(18,2),
    accrualDaily Decimal(18,2),
    accrualProjected Decimal(18,2),
    accrualPast Decimal(18,2),
    calculatedAt DateTime,
    ead Decimal(18,2),
    spread Decimal(18,2)

    ) ENGINE = ReplacingMergeTree(snapVersion)
    ORDER BY (id, snapId)
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_view(store: Store):
    print(f"Creating {Tables.RISKVIEW.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISKVIEW.value} (
        id String,
        eventId Int64,
        snapVersion Int64,
        snapId String,
        asOfDate Date,
        status LowCardinality(String),
        book LowCardinality(String),
        trade_dt Date,
        settlementDt Date,
        maturityDt Date,
        notionalCcy Decimal(18,2),
        ccy LowCardinality(String),
        counterparty LowCardinality(String),
        instrumentId String,
        updatedAt DateTime,
        cpSector LowCardinality(String),
        cpIndustry LowCardinality(String),
        cpRating LowCardinality(String),
        hmsBook LowCardinality(String),
        hmsTrader LowCardinality(String),
        hmsDesk LowCardinality(String),
        instrumentName String,
        instrumentCurrency LowCardinality(String),
        instrumentCountry LowCardinality(String),
        instrumentSector LowCardinality(String),
        accrualDaily Decimal(18,2),
        accrualProjected Decimal(18,2),
        accrualPast Decimal(18,2),
        cashOut Decimal(18,2),
        margin Decimal(18,2),
        fxSpot Decimal(18,2),
        marginFixed Decimal(18,2),
        spread Decimal(18,2),
        ead Decimal(18,2)        

    ) ENGINE = ReplacingMergeTree(snapVersion)
    ORDER BY (id,snapId)
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_view_mv(store: Store):
    print(f"Creating {Tables.RISKVIEW_MV.value} materialized view")
    query = f"""
    CREATE MATERIALIZED VIEW risk_view_mv TO risk_view
    AS SELECT 
        r.id as id,
        r.eventId as eventId,
        r.snapId as snapId,
        r.snapVersion as snapVersion,
        r.asOfDate as asOfDate,
        r.status as status,
        r.book as book,
        r.tradeDt as trade_dt,
        r.settlementDt as settlementDt,
        r.maturityDt as maturityDt,
        r.notionalCcy as notionalCcy,
        r.ccy as ccy,   
        r.counterparty as counterparty,
        r.instrumentId as instrumentId,
        r.calculatedAt as updatedAt,
     
        cp.cbSector as cpSector,
        cp.cbSector as cpIndustry,
        cp.riskRatingCrr as cpRating,
        hms.book as hmsBook,
        hms.trader as hmsTrader,
        hms.desk as hmsDesk,
        inst.name as instrumentName,
        inst.currency as instrumentCurrency,
        inst.country as instrumentCountry,
        inst.sector as instrumentSector,
        r.accrualDaily,
        r.accrualProjected,
        r.accrualPast,
        r.cashOut,
        r.margin,
        r.fxSpot,
        r.marginFixed,
        r.spread,
        r.ead

    FROM {Tables.RISK.value} as r 
    INNER JOIN {Tables.COUNTERPARTIES.value} cp ON r.counterparty = cp.id
    INNER JOIN {Tables.HMSBOOKS.value} hms ON r.book = hms.book
    INNER JOIN {Tables.INSTRUMENTS.value} inst ON r.instrumentId = inst.id
    """
    store.client.command(query)



@task(retries=0, cache_key_fn=None, persist_result=False)
def create_overrides(store: Store):
    print(f"Creating {Tables.OVERRIDES.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.OVERRIDES.value} (
        id String,
        version UInt8,
        type LowCardinality(String),
        newValue String,    
        previousValue String,
        updatedAt DateTime,
        updatedBy String,
        comments String,
        isActive Boolean default 1,
    ) ENGINE = MergeTree()
    ORDER BY (type,updatedAt,id);
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_jobs_table(store: Store):
    print(f"Creating {Tables.JOBS.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.JOBS.value} (
        id String,
        eventId Int64,
        jobType LowCardinality(String),
        snapId String,
        snapVersion UInt8,
        status LowCardinality(String),
        createdAt DateTime,
        completedAt Nullable(DateTime)
    ) ENGINE = ReplacingMergeTree(snapVersion)
    ORDER BY (snapId,jobType);
    """
    store.client.command(query)


def main():
    store = Store()
    create_db(store)
    create_jobs_table(store)
    create_hms_tables(store) 
    create_counterparty_tables(store)
    create_instruments_tables(store)
    create_trades_tables(store)
    create_risk_tables(store)
    create_risk_view(store)
    create_risk_view_mv(store)
    create_overrides(store)
   
    store.close()

if __name__ == "__main__":
    main()
