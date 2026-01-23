from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from sqlalchemy.orm import Session
from typing import List
import base64

from src.db import get_db, Reports, Documents

app = FastAPI()

# Mount static files directory
app.mount("/static", StaticFiles(directory="src/static"), name="static")



# Root: two links
from fastapi.responses import HTMLResponse

@app.get("/", response_class=FileResponse)
async def root():
    return FileResponse("src/static/auth/index.html")

# /it: show the iframe page
@app.get("/SRC")
async def src():
    return FileResponse("src/static/src/index.html")

@app.get("/ANAL")
async def anal():
    return FileResponse("src/static/anal/index.html")

# /executive: show an empty page
@app.get("/executive")
async def executive():
    return FileResponse("src/static/executive/index.html")

# Endpoint to expose the HOST environment variable as JSON
import os

#@app.get("/host-url")
#async def host_url():
#    return {"url": os.environ.get("HOST", "")}
#    # return {"url": "https://occupational-gwenore-vt-project-054c82e6.koyeb.app"}

@app.get("/src-url")
async def src_url():
    return {"url": os.environ.get("HOST1", "")}

@app.get("/anal-url")
async def anal_url():
    return {"url": os.environ.get("HOST2", "")}

# API Endpoints for Executive Dashboard

@app.get("/api/reports")
async def get_reports(db: Session = Depends(get_db)):
    """Get all reports"""
    reports = db.query(Reports).order_by(Reports.created_at.desc()).all()
    return [{"id": r.id, "created_at": r.created_at.isoformat()} for r in reports]


@app.get("/api/reports/{report_id}/documents")
async def get_report_documents(report_id: int, db: Session = Depends(get_db)):
    """Get all documents for a specific report"""
    documents = db.query(Documents).filter(Documents.report_id == report_id).all()
    result = []
    for doc in documents:
        result.append({
            "id": doc.id,
            "name": doc.name,
            "file": base64.b64encode(doc.file).decode('utf-8') if doc.file else None
        })
    return result


@app.get("/api/documents/{document_id}/download")
async def download_document(document_id: int, db: Session = Depends(get_db)):
    """Download a specific document"""
    document = db.query(Documents).filter(Documents.id == document_id).first()
    if not document:
        return JSONResponse(status_code=404, content={"error": "Document not found"})
    
    return Response(
        content=document.file,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={document.name}"}
    )



