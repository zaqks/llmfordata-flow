import feedparser
from datetime import datetime, timedelta
from urllib.parse import urlencode
from ._tools import insert_datasource

N_DAYS = 7

BASE_URL = "http://export.arxiv.org/api/query?"

KEYWORDS = [
    "data pipeline", "ETL", "data lake", "warehouse",
    "LLM", "agent", "retrieval", "RAG"
]

CATEGORIES = ["cs.AI", "cs.LG", "cs.DB", "stat.ML"]

from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline


def build_query():
    kw = " OR ".join(f'all:"{k}"' for k in KEYWORDS)
    cat = " OR ".join(f"cat:{c}" for c in CATEGORIES)
    return f"({kw}) AND ({cat})"


@task
async def main():
    logger = get_logger()
    since = datetime.utcnow() - timedelta(days=N_DAYS)

    query = {
        "search_query": build_query(),
        "start": 0,
        "max_results": 200,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    feed = feedparser.parse(BASE_URL + urlencode(query))

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
)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
