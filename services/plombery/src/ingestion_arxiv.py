import feedparser
from datetime import datetime, timedelta
from urllib.parse import urlencode
from _tools import insert_datasource

N_DAYS = 7
OUTPUT = "arxiv.csv"

BASE_URL = "http://export.arxiv.org/api/query?"

KEYWORDS = [
    "data pipeline", "ETL", "data lake", "warehouse",
    "LLM", "agent", "retrieval", "RAG"
]

CATEGORIES = ["cs.AI", "cs.LG", "cs.DB", "stat.ML"]

def build_query():
    kw = " OR ".join(f'all:"{k}"' for k in KEYWORDS)
    cat = " OR ".join(f"cat:{c}" for c in CATEGORIES)
    return f"({kw}) AND ({cat})"

def main():
    since = datetime.utcnow() - timedelta(days=N_DAYS)

    query = {
        "search_query": build_query(),
        "start": 0,
        "max_results": 200,
        "sortBy": "submittedDate",
        "sortOrder": "descending"
    }

    feed = feedparser.parse(BASE_URL + urlencode(query))

    extracted = 0
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
            insert_datasource(data)
        except Exception:
            # ignore insert errors and continue
            pass
        extracted += 1

    print(f"Extracted {extracted} rows")

if __name__ == "__main__":
    main()
