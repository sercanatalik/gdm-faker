import uuid,time
import random
from datetime import datetime
from clickhouse_connect import get_client
from decimal import Decimal
import clickhouse_connect
from dataclasses import dataclass
from typing import Optional
import polars as pl

client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='default', password='')

@dataclass
class Job:
    id: str
    snap_id: str
    snap_version: int
    job_type: str
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    
    @classmethod
    def create_intraday(cls, version: int, snapId: str) -> 'Job':
        return cls(
            id=str(uuid.uuid4()),
            snap_id=snapId, 
            snap_version=version,
            job_type='INTRADAY',
            status='RUNNING',
            created_at=datetime.now(),
           
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
            'snapId': self.snap_id,
            'snapVersion': self.snap_version,
            'jobType': self.job_type,
            'status': self.status,
            'createdAt': self.created_at,
            'completedAt': self.completed_at if self.completed_at else datetime.now()
        }

def generate_fo_risk_data(client,snapId,snapVersion):
    trades = client.query("SELECT * FROM trades")
    
    risk_data = []
    for trade in trades.named_results():
        # Calculate some base values for consistency
        notional_amount = Decimal(str(trade['notionalAmount']))
        fx_spot = Decimal(str(random.uniform(0.5, 2.0)))
        spread = Decimal(str(trade['financingSpread']))
        
        risk_record = {
            'id': trade['id'],
            'version': snapVersion,
            'status': random.choice(['ACTIVE', 'PENDING', 'SETTLED']),
            'book': trade['book'],
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
            'counterparty': trade['counterparty'],
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
            'snapId': snapId,
            'asOfDate': datetime.now().date(),
            'spread': spread
        }
        risk_data.append(risk_record)
    
    return risk_data

def insert_fo_risk_data(client, risk_data):
    df = pl.DataFrame(risk_data)
    # Get columns directly from DataFrame schema
    columns = df.columns
    pdf = df.to_pandas()
    client.insert_df("risk", pdf, column_names=columns)

def create_job(client, snapId: str) -> Job:
    latest_version = client.query(f"SELECT MAX(snapVersion) FROM jobs WHERE snapId = '{snapId}'").first_row[0]
    version = 1 if latest_version is None else latest_version + 1
    
    job = Job.create_intraday(version, snapId)
    df = pl.DataFrame([job.to_dict()])
    columns = df.columns
    pdf = df.to_pandas()
    client.insert_df("jobs", pdf, column_names=columns)
    return job

def update_job_status(client, job: Job) -> None:
    df = pl.DataFrame([job.to_dict()])
    # Get columns directly from DataFrame schema
    columns = df.columns
    pdf = df.to_pandas()
    client.insert_df("jobs", pdf, column_names=columns)

if __name__ == "__main__":
    while True:
        job = None
        try:
            # Create a new job
            snapId = 'LIVE'+datetime.now().strftime("%Y%m%d")
            job = create_job(client,snapId)
            
            # Generate and insert risk data
            risk_data = generate_fo_risk_data(client, job.snap_id, job.snap_version)
            insert_fo_risk_data(client, risk_data)
            print(f"Inserted {len(risk_data)} risk records", datetime.now())
            # Update job status to COMPLETED
            job.complete()
            update_job_status(client, job)
            print(f"Completed job {job.id}", datetime.now())
            
        except Exception as e:
            if job:
                job.fail()
                update_job_status(client, job)
            print(f"Error: {str(e)}")
            
        time.sleep(5)