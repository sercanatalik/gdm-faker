import asyncio
import random
import time
from dataclasses import dataclass
from typing import List, Dict, Optional
import redis.asyncio as redis
from datetime import datetime

@dataclass
class PricingConfig:
    redis_host: str = 'localhost'
    redis_port: int = 6379
    pool_size: int = 100
    pool_timeout: int = 30
    batch_size: int = 100
    update_interval: int = 1
    retry_attempts: int = 3

class InstrumentPricingService:
    def __init__(self, config: Optional[PricingConfig] = None):
        self.config = config or PricingConfig()
        self.redis_pool = redis.ConnectionPool(
            host=self.config.redis_host,
            port=self.config.redis_port,
            max_connections=self.config.pool_size
        )
        self.redis_client: Optional[redis.Redis] = None
        self.PRICE_PREFIX = 'price:'

    async def connect_redis(self) -> None:
        # retry = Retry(ExponentialBackoff(), self.config.retry_attempts)
        self.redis_client = redis.Redis(
            connection_pool=self.redis_pool,
           
        )
        await self.redis_client.flushdb(asynchronous=True)

    async def close_redis(self) -> None:
        if self.redis_client:
            await self.redis_client.aclose()
            self.redis_client = None

    async def mock_pricing_service(self, instrument_id: str) -> Dict[str, float]:
        """Simulate external pricing service with extended price information"""
        
        last_price = round(random.uniform(50, 200), 2)
        spread = round(random.uniform(0.01, 0.05), 2)
        return {
            'last': last_price,
            'bid': round(last_price - spread/2, 2),
            'ask': round(last_price + spread/2, 2),
            'spread': spread,
            'yest': round(last_price * (1 + random.uniform(-0.05, 0.05)), 2),
            'timestamp': datetime.now().isoformat()
        }

    async def update_instrument_batch(self, instrument_ids: List[str]) -> None:
        """Update prices for a batch of instruments"""
        try:
            price_updates = {}
            for inst_id in instrument_ids:
                price_key = f'{self.PRICE_PREFIX}{inst_id}'
                # Get current last price
                current_last = await self.redis_client.hget(price_key, 'last')
                
                # Get new price data
                price_data = await self.mock_pricing_service(inst_id)
                
                # Only update if price has changed or no previous price exists
                if current_last is None or float(current_last) != price_data['last']:
                    price_updates[price_key] = price_data

            # Only execute pipeline if there are updates
            if price_updates:
                async with self.redis_client.pipeline() as pipe:
                    for key, data in price_updates.items():
                        await pipe.hset(key, mapping=data)
                    await pipe.execute()
        except Exception as e:
            raise

    async def update_all_instrument_prices(self, instrument_ids: List[str]) -> None:
        """Concurrently update prices for all instruments in batches"""
        if not self.redis_client:
            await self.connect_redis()

        # Process in batches
        for i in range(0, len(instrument_ids), self.config.batch_size):
            batch = instrument_ids[i:i + self.config.batch_size]
            await self.update_instrument_batch(batch)

    async def get_instrument_details(self) -> Dict[str, Dict]:
        """Retrieve all instrument prices"""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        details = {}
        price_keys = await self.redis_client.keys(f'{self.PRICE_PREFIX}*')
        
        async with self.redis_client.pipeline() as pipe:
            for key in price_keys:
                await pipe.hgetall(key)
            results = await pipe.execute()

        for key, price_data in zip(price_keys, results):
            instrument_id = key.decode().replace(self.PRICE_PREFIX, '')
            details[instrument_id] = {
                'prices': {k.decode(): float(v.decode()) for k, v in price_data.items()}
            }
        return details

async def main() -> None:
    config = PricingConfig()
    instrument_ids = [f'INST_{i}' for i in range(1, 100)]
    pricing_service = InstrumentPricingService(config)
    
    try:
        await pricing_service.connect_redis()

        while True:
            start_time = time.time()
            await pricing_service.update_all_instrument_prices(instrument_ids)
            print(f"Updated prices for {len(instrument_ids)} instruments in {time.time() - start_time:.2f} seconds")
            print('Current time:', datetime.now().isoformat())
            await asyncio.sleep(config.update_interval)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        raise
    finally:
        await pricing_service.close_redis()

if __name__ == '__main__':
    asyncio.run(main())