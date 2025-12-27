# from src.example import *
from src.ingestion_arxiv import *
# from src.ingestion_vldb_sigmod import *
import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "plombery:get_app", reload=True, factory=True, port=8000, host="0.0.0.0"
    )
