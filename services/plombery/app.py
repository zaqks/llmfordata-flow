# from src.example import *
# from src.ingestion_vldb_sigmod import *

from src.flows.ingestion.ingestion_arxiv import *
from src.flows.ingestion.ingestion_huggingface import *
from src.flows.ingestion.ingestion_nvidia import *

from src.flows.ingestion.ingestion_databricks import *

# from src.flows.ingestion.ingestion_paperswithcode import *
# from src.flows.ingestion.ingestion_semanticscholar import *
from src.flows.ingestion.ingestion_snowflake import *  # empty data

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
