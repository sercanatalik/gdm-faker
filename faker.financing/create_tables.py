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
    version Int64,
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
    snapId String, 
    asOfDate Date,
    spread Decimal(18,2)

    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (id, version, snapId)
    PRIMARY KEY (id)
    SETTINGS index_granularity = 8192;
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_view(store: Store):
    print(f"Creating {Tables.RISKVIEW.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.RISKVIEW.value} (
        id String,
        version Int64,
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
        snapId String,
        asOfDate Date,
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

    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (id,version,snapId)
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_view_mv(store: Store):
    print(f"Creating {Tables.RISKVIEW_MV.value} materialized view")
    query = f"""
    CREATE MATERIALIZED VIEW {Tables.RISKVIEW_MV.value} TO {Tables.RISKVIEW.value}
    AS SELECT
    r.id as id,
    r.version as version,
    r.status as status,
    r.book as book,
    r.tradeDt as tradeDt,
    r.settlementDt as settlementDt,
    r.maturityDt as maturityDt,
    r.notionalCcy as notionalCcy,
    r.firstReset as firstReset,
    r.subType as subType,
    r.productType as productType,
    r.ccy as ccy,
    r.counterparty as counterparty,
    r.calculatedAt as calculatedAt,
    r.snapId as snapId,
    r.notionalAmount as notionalAmount,
    r.asOfDate as asOfDate,
    r.cashOut as cashOut,
    cp.cbSector AS cpSector,
    cp.riskRatingCrr AS cpRating,
    hms.book AS hmsBook,
    hms.trader AS hmsTrader,
    hms.desk AS hmsDesk,
    r.accrualDaily as accrualDaily,
    r.accrualPast as accrualPast,
    r.accrualProjected as accrualProjected,
    r.ead as ead,
    r.fxSpot as fxSpot,
    r.spread as spread,
    inst.name as instrumentName,
    inst.currency as instrumentCurrency,
    inst.country as instrumentCountry,
    inst.sector as instrumentSector
    
    FROM {Tables.RISK.value} r
    INNER JOIN {Tables.COUNTERPARTIES.value} cp ON r.counterparty = cp.id
    INNER JOIN {Tables.HMSBOOKS.value} hms ON r.book = hms.book
    INNER JOIN {Tables.INSTRUMENTS.value} inst ON r.instrumentId = inst.id
    """
    store.client.command(query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_risk_aggregating_view(store: Store):
    print(f"Creating {Tables.RISK_AGGREGATING_VIEW.value} table")
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
    store.client.command(query)

    print(f"Creating {Tables.RISK_AGGREGATING_VIEW_MV.value} materialized view")
    mv_query = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {Tables.RISK_AGGREGATING_VIEW_MV.value} TO {Tables.RISK_AGGREGATING_VIEW.value}
    AS SELECT
        hmsDesk,
        hmsTrader,
        book,
        asOfDate,
        sum(notionalCcy) AS totalNotionalAmount,
        sum(accrualDaily) AS totalDailyAccrual,
        sum(cashOut) AS totalCashout,
        sum(ead) AS totalEad,
        sum(accrualProjected) AS totalProjectedAccrual,
        sum(accrualPast) AS totalPastAccrual,
        max(version) AS version
    FROM {Tables.RISKVIEW.value}
    GROUP BY hmsDesk, hmsTrader, book, asOfDate;
    """
    store.client.command(mv_query)

@task(retries=0, cache_key_fn=None, persist_result=False)
def create_overrides(store: Store):
    print(f"Creating {Tables.OVERRIDES.value} table")
    query = f"""
    CREATE TABLE IF NOT EXISTS {Tables.OVERRIDES.value} (
        id String,
        type LowCardinality(String),
        newValue String,    
        previousValue String,
        updatedAt DateTime,
        updatedBy String,
        comments String,
        isActive Boolean default 1
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
        jobType LowCardinality(String),
        snapId String,
        snapVersion UInt8,
        status LowCardinality(String),
        createdAt DateTime,
        completedAt Nullable(DateTime)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (snapId,snapVersion);
    """
    store.client.command(query)


def main():
    store = Store()
    create_db(store)
    create_hms_tables(store) 
    create_counterparty_tables(store)
    create_instruments_tables(store)
    create_trades_tables(store)
    create_risk_tables(store)
    create_risk_view(store)
    create_risk_view_mv(store)
    create_risk_aggregating_view(store)
    create_overrides(store)
    create_jobs_table(store)
    store.close()

if __name__ == "__main__":
    main()
