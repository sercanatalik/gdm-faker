import polars as pl
from datetime import datetime
from create_index_tables import Store, INDEX_TABLES
import uuid
basket_data1 = [
    {
        "id": str(uuid.uuid4()),
        "sym": "AA1513668917",
        "weight": 50,
        "category": "US Treasuries",
        "name": "H.UST.10Y",
        "model": "yield",
        "description": "US Treasuries basket",
        "ticker": "H.USTECH",
        "updatedAt": datetime.now(),
        "asofDate": datetime.now().date(),
    },
    {
        "id": str(uuid.uuid4()),
        "sym": "AA5684563385",
        "weight": 50,
        "category": "US Treasuries",
        "name": "H.UST.10Y",
        "model": "yield",
        "description": "US Treasuries basket",
        "ticker": "H.USTECH",
        "updatedAt": datetime.now(),
        "asofDate": datetime.now().date(),
    }
]

basket_data2 = [
     {
        "id": str(uuid.uuid4()),
        "sym": "AA5684563385",
        "weight": 100,
        "category": "IG Corporate",
        "name": "H.IG.10Y",
        "model": "spread",
        "description": "IG Corporate basket",
        "ticker": "H.IGTECH",
        "updatedAt": datetime.now(),
        "asofDate": datetime.now().date(),
    }
]   

df = pl.DataFrame(basket_data1)

df2 = pl.DataFrame(basket_data2)


store = Store()
store.client.insert_arrow(INDEX_TABLES.REF_BASKETDEF.value, df.to_arrow())
store.client.insert_arrow(INDEX_TABLES.REF_BASKETDEF.value, df2.to_arrow())
store.close()
