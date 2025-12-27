import requests
from bs4 import BeautifulSoup
from datetime import datetime
from ._tools import insert_datasource
from typing import Dict
from pydantic import BaseModel, Field, validator
import json

from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline


class InputParams(BaseModel):
    urls: Dict[str, str] = Field(
        json.dumps({
            "vldb": "https://www.vldb.org/pvldb/volumes/",
            "sigmod": "https://sigmod.org/sigmod-2024-program/",
        }),
        alias="URLS",
    )

    @validator('urls', pre=True)
    def parse_urls(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    class Config:
        allow_population_by_field_name = True


@task
async def main(params: InputParams | None = None):
    if params is None:
        params = InputParams()

    logger = get_logger()
    extracted = 0
    added = 0
    for source, url in params.urls.items():
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a"):
            title = a.text.strip()
            link = a.get("href", "")

            if len(title) < 10 or not link:
                continue

            data = {
                "source": source,
                "id": link,
                "title": title,
                "abstract_or_summary": "",
                "authors": "",
                "date": datetime.utcnow().date().isoformat(),
                "url": link,
                "tags": "conference paper",
            }

            try:
                inserted = insert_datasource(data)
            except Exception:
                # swallow DB errors here to allow continuing; insertion helpers
                # already deduplicate by URL
                inserted = False

            extracted += 1
            if inserted:
                added += 1

    logger.info("Extracted %s rows from VLDB/SIGMOD sources", extracted)
    logger.info("Inserted %s new rows into DB", added)
    return {"extracted": extracted, "inserted": added}


register_pipeline(
    id="vldb_sigmod_ingestion",
    description="Ingest items from VLDB and SIGMOD pages",
    tasks=[main],
    triggers=[
        Trigger(
            id="hourly",
            name="Hourly",
            description="Run the pipeline every hour",
            schedule=IntervalTrigger(hours=1),
        ),
    ],
    params=InputParams,
)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
