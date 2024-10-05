from clickhouse_connect import get_client
from sql import create_fo_hms_table, create_fo_instrument_table

def drop_database_if_exists(client, database_name):
    client.command(f"DROP DATABASE IF EXISTS {database_name}")


def create_tables():
    # Example table creation

    # Create HMS table
    create_fo_hms_table(table_name="fo_hms")

    # Create Instrument table
    create_fo_instrument_table(table_name="fo_instrument")

    


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
    drop_database_if_exists(client, database_name)

    # Create database
    create_tables()    

    print(f"Database '{database_name}' and tables created successfully.")

    # Close the client connection
    client.close()

if __name__ == "__main__":
    main()
