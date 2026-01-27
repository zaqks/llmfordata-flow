from datetime import datetime
from random import randint

from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline

from pydantic import BaseModel


class InputParams(BaseModel):
    some_value: int


@task
async def fetch_raw_sales_data():
    """Fetch latest 50 sales of the day"""

    # using Plombery logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()

    logger.debug("Fetching sales data...")

    sales = [
        {
            "price": randint(1, 1000),
            "store_id": randint(1, 10),
            "date": datetime.today(),
            "sku": randint(1, 50),
        }
        for _ in range(50)
    ]

    logger.info("Fetched %s sales data rows", len(sales))

    # Return the results of your task to have it stored
    # and accessible on the web UI
    # If you have other tasks, the output of a task is
    # passed to the following one
    return sales


register_pipeline(
    id="sales_pipeline",
    description="Aggregate sales activity from all stores across the country",
    tasks=[fetch_raw_sales_data],
    triggers=[
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1),
        ),
    ],
    params=InputParams,
)
