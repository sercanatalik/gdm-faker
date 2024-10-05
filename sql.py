def create_fo_hms_table(table_name):
    # Create a ClickHouse client

    # SQL statement to create the fo_hms table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id UInt32,
        organization String,
        balance_sheet Float64,
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


def create_fo_instrument_table(table_name):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id UInt32,
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
