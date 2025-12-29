import uvicorn
import os

if __name__ == "__main__":
    uvicorn.run(
        "src.app:app",
        reload=os.getenv("RELOAD") != "false",
        port=8000,
        host="0.0.0.0",
        # workers=4,
    )

 