from clickhouse_connect import get_client
import pandas as pd
from generate_refdata import (
    create_fo_hms_table,
    create_fo_instrument_table,
    generate_fo_hms_data,
    generate_fo_instrument_data,
)

def drop_database_if_exists(client, database_name):
    client.command(f"DROP DATABASE IF EXISTS {database_name}")


def create_tables(client):
    # Create HMS table
    sql = create_fo_hms_table(table_name="fo_hms")
    client.command(sql)


    # Create Instrument table
    sql = create_fo_instrument_table(table_name="fo_instrument")
    client.command(sql)

def generate_and_insert_data(client):
    # Generate and insert HMS data
    hms_data = generate_fo_hms_data(num_records=100)
    
   
    df = pd.DataFrame(hms_data)
    df.set_index('id', inplace=True)
    client.insert_df("fo_hms", df.reset_index())

    # Generate and insert Instrument data
    instrument_data = generate_fo_instrument_data(num_records=2500)
    df = pd.DataFrame(instrument_data)
    df.set_index('id', inplace=True)
    client.insert_df("fo_instrument", df.reset_index())

def main():
    # ClickHouse connection details
    clickhouse_host = '127.0.0.1'
    clickhouse_port = 8123  # Note: Using HTTP port for ClickHouse Connect
    clickhouse_user = 'default'
    clickhouse_password = ''
    database_name = 'default'

    # Create ClickHouse client
    client = get_client(host=clickhouse_host, port=clickhouse_port,
                        username=clickhouse_user, password=clickhouse_password)

    # Drop database if it exists
    # drop_database_if_exists(client, database_name)

    # Create database
    client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    client.command(f"USE {database_name}")

    # Create tables
    create_tables(client)

    # Generate and insert data
    generate_and_insert_data(client)

    print(f"Database '{database_name}', tables created, and data inserted successfully.")

    # Close the client connection
    client.close()

if __name__ == "__main__":
    main()
