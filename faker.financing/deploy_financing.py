from dotenv import load_dotenv
load_dotenv()

from prefect import flow, serve
from create_tables import create_db, create_counterparty_tables, create_hms_tables, create_instruments_tables, create_trades_tables, create_risk_tables, create_risk_view, create_risk_view_mv, create_overrides, create_jobs_table, Store
from generate_refdata import load_hms_data, load_counterparty_data, load_instrument_data
from generate_trades import generate_fo_trades_trs, load_trades_to_clickhouse
from generate_risk import run_risk
from datetime import timedelta


@flow(log_prints=True, persist_result=False, cache_result_in_memory=False)
def drop_tables():
    store = Store()
    create_db(store)
    store.close()


@flow(log_prints=True, persist_result=False, cache_result_in_memory=False)
def create_tables():
    store = Store()
    create_jobs_table(store)
    create_hms_tables(store)
    create_counterparty_tables(store)
    create_instruments_tables(store)
    create_trades_tables(store)
    create_risk_tables(store)
    create_risk_view(store)
    create_risk_view_mv(store)
    create_overrides(store)
    create_jobs_table(store)
    store.close()


@flow(log_prints=True, persist_result=False, cache_result_in_memory=False)
def load_refdata():
    store = Store()
    load_hms_data(store)
    load_counterparty_data(store)
    load_instrument_data(store)
    store.close()


@flow(log_prints=True, persist_result=False, cache_result_in_memory=False)
def load_trades():
    store = Store()
    data = generate_fo_trades_trs(store, num_records=1000)
    load_trades_to_clickhouse(store, data)
    store.close()


@flow(log_prints=True, persist_result=False, cache_result_in_memory=False)
def generate_risk():
    run_risk()


if __name__ == "__main__":

    serve(drop_tables.to_deployment(
        name="drop_tables"),
        create_tables.to_deployment(
            name="create_tables"),
        load_refdata.to_deployment(
            name="load_refdata"),
        load_trades.to_deployment(
            name="load_trades"),
        generate_risk.to_deployment(
            name="generate_risk", interval=timedelta(minutes=1))
        )

    print("Done")
