from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI()

# Mount static files directory
app.mount("/static", StaticFiles(directory="src/static"), name="static")

@app.get("/")
async def root():
    return FileResponse("src/static/index.html")
