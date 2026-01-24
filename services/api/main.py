import uvicorn
import os
from src.db import Base, engine

# Create database tables on startup
try:
    Base.metadata.create_all(engine)
    print("✅ Database tables initialized")
except Exception as e:
    print(f"⚠️  Warning: Could not create database tables: {e}")

if __name__ == "__main__":
    uvicorn.run(
        "src.app:app",
        reload=os.getenv("RELOAD") != "false",
        port=8000,
        host="0.0.0.0",
        # workers=4,
    )

 