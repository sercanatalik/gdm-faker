import clickhouse_connect
from prefect import task
import enum

class Store:
    def __init__(self):
        self.client = clickhouse_connect.get_client(host="127.0.0.1", port=8123)
    def close(self):
        self.client.close()


class INDEX_TABLES(enum.Enum):
    REF_BASKETDEF = "ref_basketdef"
    MD_INSTRUMENTS = "md_instruments"





@task(retries=0, cache_key_fn=None, persist_result=False)
def create_basketdef_table(store: Store):
    print(f"Creating {INDEX_TABLES.REF_BASKETDEF.value} table")
    query = f"""
    DROP TABLE IF EXISTS {INDEX_TABLES.REF_BASKETDEF.value};
    """
    store.client.command(query)
    query = f"""
    CREATE TABLE IF NOT EXISTS {INDEX_TABLES.REF_BASKETDEF.value} (
        id String,
        sym String,
        weight Float64,
        category String,
        name String,
        model String,
        description String,
        ticker String,
        updatedAt DateTime,
        asofDate Date

    ) ENGINE = ReplacingMergeTree(updatedAt)
    ORDER BY (sym,name,asofDate);
    """
    store.client.command(query)



def main():
    store = Store()
 
    create_basketdef_table(store)

if __name__ == "__main__":
    main()


