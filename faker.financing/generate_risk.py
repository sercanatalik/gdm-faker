import uuid,time
import random
from datetime import datetime
from clickhouse_connect import get_client
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional
import polars as pl
from create_tables import Store,Tables
client = Store().client
import numpy as np

@dataclass
class Job:
    id: str
    snapId: str
    snapVersion: int
    jobType: str
    status: str
    createdAt: datetime
    completedAt: Optional[datetime] = None
    
    @classmethod
    def create_intraday(cls, version: int, snapId: str) -> 'Job':
        return cls(
            id=str(uuid.uuid4()),
            snapId=snapId, 
            snapVersion=version,
            jobType='INTRADAY',
            status='RUNNING',
            createdAt=datetime.now(),
           
        )
    
    def complete(self) -> None:
        self.status = 'COMPLETED'
        self.completed_at = datetime.now()
    
    def fail(self) -> None:
        self.status = 'FAILED'
        self.completed_at = datetime.now()
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'snapId': self.snapId,
            'snapVersion': self.snapVersion,
            'jobType': self.jobType,
            'status': self.status,
            'createdAt': self.createdAt,
            'completedAt': self.completedAt if self.completedAt else datetime.now()
        }

def generate_fo_risk_data(client,snapId,snapVersion):
    trades = client.query("SELECT * FROM "+Tables.TRADES.value + " final")
    print(trades)
    risk_data = []
    for trade in trades.named_results():
        # Calculate some base values for consistency
        notional_amount = Decimal(str(trade['notionalAmount']))
        fx_spot = Decimal(str(random.uniform(0.5, 2.0)))
        spread = Decimal(str(trade['financingSpread']))
        
        risk_record = {
            'id': trade['id'],
            'eventId': np.int64(str(snapVersion) + str(trade['eventId'])),
            'snapId': snapId,
            'snapVersion': snapVersion,
            'asOfDate': datetime.now().date(),
            'status': random.choice(['ACTIVE', 'PENDING', 'SETTLED']),
            'book': trade['book'],
            'counterparty': trade['counterparty'],
            'tradeDt': datetime.now().date(),
            'settlementDt': datetime.now().date(),
            'maturityDt': datetime.now().date(),
            'notionalCcy': notional_amount,
            'notionalAmount': notional_amount,
            'firstReset': Decimal(str(random.uniform(0.01, 0.05))),
            'subType': random.choice(['SWAP', 'FORWARD', 'OPTION']),
            'productType': random.choice(['IR', 'FX', 'EQUITY']),
            'ccy': trade['currency'],
            'haircutManual': Decimal(str(random.uniform(0, 0.1))),
            'bondcfFactor': Decimal(str(random.uniform(0.8, 1.2))),
            'iaimAmount': notional_amount * Decimal('0.1'),
            'iaimCcy': trade['currency'],
            'side': random.choice(['BUY', 'SELL']),
            'model': random.choice(['BLACK_SCHOLES', 'MONTE_CARLO', 'BINOMIAL']),
            'notionalFundingCcy': notional_amount * fx_spot,
            'marginOis': Decimal(str(random.uniform(0, 0.02))),
            'marginFixed': Decimal(str(random.uniform(0, 0.05))),
            'marginFloat': Decimal(str(random.uniform(0, 0.03))),
            'instrumentId': trade['instrument'],
            'dtm': random.randint(1, 365),
            'tenor': random.choice(['1M', '3M', '6M', '1Y']),
            'mid': Decimal(str(random.uniform(95, 105))),
            'fxSpot': fx_spot,
            'sideFactor': random.choice(['1', '-1']),
            'notional': notional_amount,
            'ccyFunding': trade['currency'],
            'fxspotFunding': fx_spot,
            'notionalFunding': notional_amount * fx_spot,
            'iaAmount': notional_amount * Decimal('0.1'),
            'cashOut': Decimal(str(random.uniform(0, float(notional_amount)))),
            'haircut': Decimal(str(random.uniform(0, 0.1))),
            'margin': Decimal(str(random.uniform(0, 0.05))),
            'accrualDaily': spread * notional_amount / Decimal('365'),
            'accrualProjected': spread * notional_amount * Decimal('150') / Decimal('365'),
            'accrualPast': spread * notional_amount * Decimal('90') / Decimal('365'),
            'calculatedAt': datetime.now(),
            'ead': notional_amount * Decimal('0.4'),
            'spread': spread
        }
        risk_data.append(risk_record)
    
    return risk_data

def insert_fo_risk_data(client, risk_data):
    df = pl.DataFrame(risk_data)
    # Get columns directly from DataFrame schema
    columns = df.columns
    pdf = df.to_pandas()
    print(columns)
    client.insert_df(Tables.RISK.value, pdf, column_names=columns)

def create_job(client, snapId: str) -> Job:
    try:
        latest_version = client.query(f"SELECT MAX(snapVersion) FROM {Tables.JOBS.value} WHERE snapId = '{snapId}'").result_rows[0][0]
        version = 0 if latest_version is None else latest_version + 1
    except Exception as e:
        print(f"Error querying version, defaulting to 0: {str(e)}")
        version = 0
    
    print(f"Creating job with version: {version}")
    job = Job.create_intraday(version, snapId)
    df = pl.DataFrame([job.to_dict()])
    columns = df.columns
    pdf = df.to_pandas()
    client.insert_df(Tables.JOBS.value, pdf, column_names=columns)
    return job

def update_job_status(client, job: Job) -> None:
    df = pl.DataFrame([job.to_dict()])
    # Get columns directly from DataFrame schema
    columns = df.columns
    pdf = df.to_pandas()
    client.insert_df(Tables.JOBS.value, pdf, column_names=columns)


def run_risk():
    store = Store()
    snapId = 'LIVE'+datetime.now().strftime("%Y%m%d")
    job = create_job(store.client,snapId)
        # Generate and insert risk data
    risk_data = generate_fo_risk_data(store.client, job.snapId, job.snapVersion)
    insert_fo_risk_data(store.client, risk_data)
    print(f"Inserted {len(risk_data)} risk records", datetime.now())
    job.complete()
    update_job_status(store.client, job)
    print(f"Completed job {job.id}", datetime.now())
    store.close()



if __name__ == "__main__":
    run_risk()