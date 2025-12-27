import requests
from bs4 import BeautifulSoup
from datetime import datetime
from _tools import insert_datasource

OUTPUT = "vldb_sigmod.csv"

URLS = {
    "vldb": "https://www.vldb.org/pvldb/volumes/",
    "sigmod": "https://sigmod.org/sigmod-2024-program/"
}

def main():
    extracted = 0
    for source, url in URLS.items():
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
                insert_datasource(data)
            except Exception:
                # swallow DB errors here to allow continuing; insertion helpers
                # already deduplicate by URL
                pass
            extracted += 1

    print(f"Extracted {extracted} rows")

if __name__ == "__main__":
    main()
