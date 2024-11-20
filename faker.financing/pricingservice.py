import asyncio
import random
import time
import redis.asyncio as redis
from typing import List, Dict

class InstrumentPricingService:
    def __init__(self, pool_size: int = 100, pool_timeout: int = 30):
        self.pool_size = pool_size
        self.pool_timeout = pool_timeout
        self.redis_pool = redis.ConnectionPool(host='localhost', port=6379)
        self.redis_client = None
        self.INSTRUMENT_PREFIX = 'instrument:'
        self.PRICE_PREFIX = 'price:'
        self.CALC_TIME_PREFIX = 'calc_time:'

    async def connect_redis(self):
        self.redis_client = redis.Redis(connection_pool=self.redis_pool)

    async def close_redis(self):
        if self.redis_client:
            await self.redis_client.aclose()

    async def mock_pricing_service(self, instrument_id: str) -> float:
        """Simulate external pricing service"""
        await asyncio.sleep(random.uniform(0.1, 0.5))
        return round(random.uniform(50, 200), 2)

    async def update_instrument_price(self, instrument_id: str):
        """Fetch price for a single instrument and update in Redis with calculation time"""
        try:
            # Start timing
            start_time = time.time()

            # Fetch new price
            new_price = await self.mock_pricing_service(instrument_id)
            
            # Calculate processing time
            calc_time = time.time() - start_time
            
            # Update instrument and price using prefixed keys
            instrument_key = f'{self.INSTRUMENT_PREFIX}{instrument_id}'
            price_key = f'{self.PRICE_PREFIX}{instrument_id}'
            calc_time_key = f'{self.CALC_TIME_PREFIX}{instrument_id}'
            
            # Set instrument existence, price, and calculation time
            async with self.redis_client.pipeline() as pipe:
                await pipe.set(instrument_key, instrument_id)
                await pipe.set(price_key, str(new_price))
                await pipe.set(calc_time_key, str(round(calc_time * 1000, 2)))
                await pipe.execute()
            
            print(f"Updated {instrument_id}: ${new_price} (Calc Time: {round(calc_time * 1000, 2)} ms)")
        except Exception as e:
            print(f"Error updating {instrument_id}: {e}")

    async def update_all_instrument_prices(self, instrument_ids: List[str]):
        """Concurrently update prices for all instruments"""
        if not self.redis_client:
            await self.connect_redis()

        # Create tasks for concurrent price updates
        tasks = [self.update_instrument_price(inst_id) for inst_id in instrument_ids]
        await asyncio.gather(*tasks)

    async def get_instrument_details(self):
        """Retrieve all instrument prices and calculation times"""
        details = {}
        price_keys = await self.redis_client.keys(f'{self.PRICE_PREFIX}*')
        
        for key in price_keys:
            instrument_id = key.decode().replace(self.PRICE_PREFIX, '')
            price = await self.redis_client.get(key)
            calc_time_key = f'{self.CALC_TIME_PREFIX}{instrument_id}'
            calc_time = await self.redis_client.get(calc_time_key)
            
            details[instrument_id] = {
                'price': price.decode(),
                'calc_time_ms': calc_time.decode() if calc_time else 'N/A'
            }
        return details

async def main():
    # Sample instrument IDs
    instrument_ids = [f'INST_{i}' for i in range(1, 10000)]

    # Initialize service
    pricing_service = InstrumentPricingService()
    
    try:
        # Connect to Redis
        await pricing_service.connect_redis()

        # Total service start time
        total_start_time = time.time()

        # Update all instrument prices concurrently
        await pricing_service.update_all_instrument_prices(instrument_ids)

        # Calculate total service execution time
        total_exec_time = time.time() - total_start_time
        print(f"\nTotal Service Execution Time: {round(total_exec_time * 1000, 2)} ms")

        # Retrieve and print updated prices and calculation times
        details = await pricing_service.get_instrument_details()
        # print("\nInstrument Details:")
        # for inst_id, info in details.items():
        #     print(f"{inst_id}: Price=${info['price']}, Calc Time={info['calc_time_ms']} ms")

    finally:
        # Ensure Redis connection is closed
        await pricing_service.aclose()

if __name__ == '__main__':
    asyncio.run(main())