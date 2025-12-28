# from src.example import *
# from src.ingestion_vldb_sigmod import *

from src.flows.ingestion.ingestion_arxiv import *
from src.flows.analysis.analysis_llm import *
from src.flows.gen.report_generation import *

import uvicorn
import os

if __name__ == "__main__":

    uvicorn.run(
        "plombery:get_app",
        reload=os.getenv("RELOAD") != "false",
        factory=True,
        port=8000,
        host="0.0.0.0",
        # workers=4,
    )
