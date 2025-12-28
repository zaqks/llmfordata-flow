# from src.example import *
# from src.ingestion_vldb_sigmod import *

from src.flows.ingestion.ingestion_arxiv import *
from src.flows.analysis.analysis_llm import *
from src.flows.placeholder.placeholder import keep_alive

import threading
import asyncio


import uvicorn



def start_keep_alive():
    asyncio.run(keep_alive())

if __name__ == "__main__":
    t = threading.Thread(target=start_keep_alive, daemon=True)
    t.start()
    uvicorn.run(
        "plombery:get_app",
        reload=True,
        factory=True,
        port=8000,
        host="0.0.0.0",
        workers=2,
    )
