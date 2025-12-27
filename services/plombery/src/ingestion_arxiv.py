import feedparser
from datetime import datetime, timedelta
from urllib.parse import urlencode
from ._tools import insert_datasource
from typing import List
from pydantic import BaseModel, Field

from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline
from pydantic import BaseModel, Field, validator


class InputParams(BaseModel):
    n_days: int = Field(7, alias="N_DAYS")
    base_url: str = Field("http://export.arxiv.org/api/query?", alias="BASE_URL")
    keywords: List[str] = Field(
        [
            "data pipeline",
            "ETL",
            "data lake",
            "warehouse",
            "LLM",
            "agent",
            "retrieval",
            "RAG",
        ],
        alias="KEYWORDS",
    )
    categories: List[str] = Field(
        ["cs.AI", "cs.LG", "cs.DB", "stat.ML"], alias="CATEGORIES"
    )
    max_results: int = Field(200, alias="MAX_RESULTS")

    @validator('keywords', pre=True)
    def parse_keywords(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(',')]
        return v

    @validator('categories', pre=True)
    def parse_categories(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(',')]
        return v

    class Config:
        allow_population_by_field_name = True


def _build_query(keywords: list[str], categories: list[str]) -> str:
    kw = " OR ".join(f'all:"{k}"' for k in keywords)
    cat = " OR ".join(f"cat:{c}" for c in categories)
    return f"({kw}) AND ({cat})"


@task
async def main(params: InputParams | None = None):
    if params is None:
        params = InputParams()

    logger = get_logger()
    since = datetime.utcnow() - timedelta(days=params.n_days)

    query = {
        "search_query": _build_query(params.keywords, params.categories),
        "start": 0,
        "max_results": params.max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    feed = feedparser.parse(params.base_url + urlencode(query))

    extracted = 0
    added = 0
    for e in feed.entries:
        published = datetime.strptime(e.published, "%Y-%m-%dT%H:%M:%SZ")
        if published < since:
            continue

        data = {
            "source": "arxiv",
            "id": e.id.split("/")[-1],
            "title": e.title.replace("\n", " "),
            "abstract_or_summary": e.summary.replace("\n", " "),
            "authors": "; ".join(a.name for a in e.authors),
            "date": published.date().isoformat(),
            "url": e.link,
            "tags": "; ".join(t.term for t in e.tags),
        }

        try:
            inserted = insert_datasource(data)
        except Exception:
            # ignore insert errors and continue
            inserted = False

        extracted += 1
        if inserted:
            added += 1

    logger.info("Extracted %s rows from arXiv", extracted)
    logger.info("Inserted %s new rows into DB", added)
    return {"extracted": extracted, "inserted": added}


register_pipeline(
    id="arxiv_ingestion",
    description="Ingest recent arXiv papers matching keywords",
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
