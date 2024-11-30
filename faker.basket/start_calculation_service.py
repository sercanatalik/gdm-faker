import asyncio
import threading
from typing import Optional
import redis
import random
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class PriceGeneratorService:
    def __init__(self, redis_client: redis.Redis, instruments: list[str], max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._redis = redis_client
        self._instruments = instruments

    async def _generate_price(self, instrument: str):
        price = random.uniform(90, 110)
        timestamp = datetime.now().isoformat()
        return {
            'instrument': instrument,
            'price': price,
            'timestamp': timestamp
        }

    async def _process_queue(self):
        while self._running:
            try:
                for instrument in self._instruments:
                    price_data = await self._generate_price(instrument)
                    # Publish to Redis
                    self._redis.publish('price_updates', json.dumps(price_data))
                await asyncio.sleep(1)  # Generate prices every second
            except Exception as e:
                print(f"Error generating prices: {e}")

    def _run_event_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._process_queue())

    def start(self):
        """Start the price generator service"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_event_loop)
        self._thread.daemon = True
        self._thread.start()
        print("Price generator service started")

    def stop(self):
        """Stop the price generator service"""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join()
        if self._loop:
            self._loop.stop()
        self._executor.shutdown(wait=True)
        print("Price generator service stopped")

    async def submit_calculation(self, data):
        """Submit a calculation to be processed"""
        if not self._running:
            raise RuntimeError("Service is not running")
        
        # Execute calculation in thread pool
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: asyncio.run(self._run_calculation(data))
        )

class PriceProcessorService:
    def __init__(self, redis_client: redis.Redis, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._redis = redis_client
        self._pubsub = redis_client.pubsub()

    async def _process_price(self, price_data: dict):
        # Mock calculation - replace with your actual logic
        price = price_data['price']
        return price * 1.1  # Simple 10% markup calculation

    async def _process_queue(self):
        self._pubsub.subscribe('price_updates')
        
        while self._running:
            try:
                message = self._pubsub.get_message()
                if message and message['type'] == 'message':
                    price_data = json.loads(message['data'])
                    result = await self._process_price(price_data)
                    print(f"Processed {price_data['instrument']}: {result}")
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Error processing prices: {e}")

    def _run_event_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._process_queue())

    def start(self):
        """Start the price processor service"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_event_loop)
        self._thread.daemon = True
        self._thread.start()
        print("Price processor service started")

    def stop(self):
        """Stop the price processor service"""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join()
        if self._loop:
            self._loop.stop()
        self._executor.shutdown(wait=True)
        print("Price processor service stopped")

    async def submit_calculation(self, data):
        """Submit a calculation to be processed"""
        if not self._running:
            raise RuntimeError("Service is not running")
        
        # Execute calculation in thread pool
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: asyncio.run(self._run_calculation(data))
        )

async def main():
    # Setup Redis client
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    # Create services
    instruments = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']
    generator = PriceGeneratorService(redis_client, instruments)
    processor = PriceProcessorService(redis_client)
    
    # Start services
    generator.start()
    processor.start()
    
    try:
        # Run indefinitely
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down services...")
        # Stop services
        generator.stop()
        processor.stop() 
        # Close Redis connection
        redis_client.close()

if __name__ == "__main__":
    asyncio.run(main())
