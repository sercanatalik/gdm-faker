from prefect import flow, task, serve
from prefect.client.schemas.schedules import CronSchedule
from datetime import datetime
import time
import pytz
from typing import Optional, Any
from dotenv import load_dotenv
import logging
import os
import asyncio
from pricingservice import InstrumentPricingService

# Configuration
NY_TIMEZONE = pytz.timezone('America/New_York')
BUSINESS_HOURS = {
    'start': datetime.strptime('08:00', '%H:%M').time(),
    'end': datetime.strptime('17:00', '%H:%M').time()
}
DEFAULT_RETRY_CONFIG = {
    'max_retries': 3,
    'retry_delay': 60
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@task
def check_business_hours() -> bool:
    """Check if current time is within business hours"""
    current_time = datetime.now(NY_TIMEZONE).time()
    return BUSINESS_HOURS['start'] <= current_time <= BUSINESS_HOURS['end']

@task(retries=3, retry_delay_seconds=30)
async def your_business_logic() -> bool:
    await asyncio.sleep(1)

    # Sample instrument IDs
    instrument_ids = [f'INST_{i}' for i in range(1, 10000)]

    # Initialize service with connection pool settings
    pricing_service = InstrumentPricingService(
        pool_size=100,  # Adjust based on your needs
        pool_timeout=30
    )
    
    try:
        # Connect to Redis (now using connection pool)
        await pricing_service.connect_redis()

        # Process instruments in batches to manage connections
        batch_size = 1000
        for i in range(0, len(instrument_ids), batch_size):
            batch = instrument_ids[i:i + batch_size]
            
            # Update batch of instrument prices
            await pricing_service.update_all_instrument_prices(batch)
            
            # Optional: Add small delay between batches
            await asyncio.sleep(0.1)

        # Retrieve and print updated prices
        details = await pricing_service.get_instrument_details()

    finally:
        # Ensure Redis connection is closed
        await pricing_service.close_redis()

    logger.info("Business logic completed")
    return True

@task
def handle_failure(
    error: Exception,
    retry_count: int,
    max_retries: int = DEFAULT_RETRY_CONFIG['max_retries'],
    retry_delay: int = DEFAULT_RETRY_CONFIG['retry_delay']
) -> bool:
    """Handle task failure with automatic restart"""
    if retry_count >= max_retries:
        logger.error(f"Maximum retries ({max_retries}) reached. Please check the system.")
        return False
    
    logger.error(f"Process failed with error: {str(error)}")
    logger.info(f"Attempting restart {retry_count + 1}/{max_retries} in {retry_delay} seconds...")
    time.sleep(retry_delay)
    return True

@flow(name="Business Hours Flow", retries=3)
async def business_hours_flow(
    max_retries: int = DEFAULT_RETRY_CONFIG['max_retries'],
    retry_delay: int = DEFAULT_RETRY_CONFIG['retry_delay']
):
    """Main flow that runs only during business hours"""
    if not check_business_hours():
        logger.info(f"Outside business hours at {datetime.now(NY_TIMEZONE)}, skipping execution")
        return None

    for retry_count in range(max_retries + 1):
        try:
            result = await your_business_logic()
            logger.info(f"Task executed successfully at {datetime.now(NY_TIMEZONE)}")
            return result
        except Exception as e:
            should_continue = handle_failure(e, retry_count, max_retries, retry_delay)
            if not should_continue:
                break 
    
    return False

# Schedule to run every minute
schedule = CronSchedule(
    cron="* * * * *",  # Every minute
    timezone="America/New_York"
)

def create_and_apply_deployment():
    
    serve(
        business_hours_flow.to_deployment(
            name="business-hours-deployment",
            cron='* * * * *',
            parameters={
                "max_retries": 3,
                "retry_delay": 60
            }
        )
    )

if __name__ == "__main__":
    # Remove load_dotenv() here since it's now in create_and_apply_deployment()
    create_and_apply_deployment()
    