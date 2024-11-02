-- Create the main riskFinancing table
CREATE TABLE IF NOT EXISTS riskFinancing (
    -- Primary identifiers
    id String,
    snapVersion Int64,
    instrumentId String,
    snapId String,
    asOfDate Date,

    -- Status and categorization (using LowCardinality for categorical fields)
    book LowCardinality(String),
    desk LowCardinality(String),      -- Added desk
    trader LowCardinality(String),    -- Added trader
    portfolio LowCardinality(String), -- Added portfolio
    regulatoryTmt LowCardinality(String),
    accountingTmt LowCardinality(String),
    assetClass LowCardinality(String),
    legalEntity LowCardinality(String),
    leCurrency LowCardinality(String),
    
    
    subType LowCardinality(String),
    productType LowCardinality(String),
    model LowCardinality(String),
    counterparty LowCardinality(String),

    
    -- Key dates
    tradeDt Date,
    settlementDt Date,
    maturityDt Date,
    calculatedAt DateTime,
    
    -- Trade details
    side LowCardinality(String),      -- Converting to LowCardinality as it's categorical
    tenor LowCardinality(String),     -- Converting to LowCardinality as it's categorical
    sideFactor LowCardinality(String),
    
    -- Currencies (using LowCardinality as currency codes are categorical)
    ccy LowCardinality(String),
    iaimCcy LowCardinality(String),
    notionalFundingCcy LowCardinality(String),
    ccyFunding LowCardinality(String),
    
    -- Amounts and metrics (Float64 for precision)
    notionalCcy Float64,
    firstReset Float64,
    haircutManual Float64,
    bondcfFactor Float64,
    iaimAmount Float64,
    notionalFunding Float64,
    marginOis Float64,
    marginFixed Float64,
    marginFloat Float64,
    mid Float64,
    fxSpot Float64,
    notional Float64,
    fxspotFunding Float64,
    iaAmount Float64,
    cashOut Float64,
    haircut Float64,
    margin Float64,
    accrualDaily Float64,
    accrualProjected Float64,
    
    -- Technical fields
    dtm Int64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(tradeDt)
ORDER BY (desk, trader, portfolio, id, version, snapId)  -- Modified ordering to include new fields
SETTINGS index_granularity = 8192;

-- Materialized view for desk/trader analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS riskFinancingDeskMv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(tradeDt)
ORDER BY (tradeDt, desk, trader, portfolio)
AS SELECT
    tradeDt,
    desk,
    trader,
    portfolio,
    productType,
    ccy,
    count() as positionCount,
    uniqExact(counterparty) as counterpartyCount,
    sum(notionalCcy) as totalNotional,
    sum(margin) as totalMargin,
    sum(cashOut) as totalCashOut,
    sum(accrualDaily) as totalDailyAccrual
FROM riskFinancing
GROUP BY
    tradeDt,
    desk,
    trader,
    portfolio,
    productType,
    ccy;

-- Materialized view for portfolio risk metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS riskFinancingPortfolioMv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(tradeDt)
ORDER BY (tradeDt, portfolio, productType)
AS SELECT
    tradeDt,
    portfolio,
    productType,
    ccy,
    count() as tradeCount,
    sum(notionalCcy) as grossNotional,
    sum(if(side = 'BUY', notionalCcy, 0)) as longPositions,
    sum(if(side = 'SELL', notionalCcy, 0)) as shortPositions,
    sum(margin) as totalMargin,
    sum(marginOis + marginFixed + marginFloat) as totalMarginComponents,
    sum(haircut) as totalHaircut,
    sum(accrualProjected) as projectedAccrual
FROM riskFinancing
GROUP BY
    tradeDt,
    portfolio,
    productType,
    ccy;

-- Materialized view for desk risk limits monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS riskFinancingDeskLimitsMv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(tradeDt)
ORDER BY (tradeDt, desk, ccy)
AS SELECT
    tradeDt,
    desk,
    ccy,
    count() as activePositions,
    uniqExact(trader) as activeTraders,
    uniqExact(portfolio) as activePortfolios,
    uniqExact(counterparty) as activeCounterparties,
    sum(notionalCcy) as totalExposure,
    max(notionalCcy) as largestPosition,
    sum(margin) as totalMargin,
    sum(if(side = 'BUY', notionalCcy, 0)) as grossLong,
    sum(if(side = 'SELL', notionalCcy, 0)) as grossShort,
    sum(if(side = 'BUY', notionalCcy, 0)) - sum(if(side = 'SELL', notionalCcy, 0)) as netPosition
FROM riskFinancing
GROUP BY
    tradeDt,
    desk,
    ccy;
    


CREATE TABLE IF NOT EXISTS counterparty (
    -- Primary identifier
    id String,
    
    -- Company identifiers
    lei LowCardinality(String),  -- Legal Entity Identifier
    ptsShortName LowCardinality(String), -- Short name/code
    customerName String, -- Full legal name
    
    -- Location information
    site LowCardinality(String),
    countryOfIncorporation LowCardinality(String),
    countryOfPrimaryOperation LowCardinality(String),
    
    -- Classification fields
    treatsParent LowCardinality(String),
    treats7 LowCardinality(String),
    masterGroup LowCardinality(String),
    cbSector LowCardinality(String),
    
    -- Risk ratings
    riskRatingCrr Float64,  -- Numerical rating
    riskRatingMoodys LowCardinality(String), -- Moody's rating (e.g., 'Aaa', 'Aa1')
    riskRatingSnP LowCardinality(String),    -- S&P rating (e.g., 'AAA', 'AA+')
     
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id

