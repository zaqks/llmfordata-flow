from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List
import base64
import os
import json

from src.db import get_db, Reports, Documents, PushSubscription

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

# Service worker for push notifications (must be served from root scope)
@app.get("/sw.js")
async def service_worker():
    return FileResponse("src/static/sw.js", media_type="application/javascript")

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

# KPI Endpoints

@app.get("/api/anal_length")
async def anal_length(db: Session = Depends(get_db)):
    """Get the difference in days between the latest and oldest report"""
    min_date = db.query(func.min(Reports.created_at)).scalar()
    max_date = db.query(func.max(Reports.created_at)).scalar()
    if min_date and max_date:
        diff = (max_date - min_date).days
        return {"value": diff}
    else:
        return {"value": 0}

@app.get("/api/n_discos")
async def n_discos(db: Session = Depends(get_db)):
    """Get the number of reports"""
    count = db.query(Reports).count()
    return {"value": count}

@app.get("/api/n_alerts")
async def n_alerts(db: Session = Depends(get_db)):
    """Get the number of reports"""
    count = db.query(Reports).count()
    return {"value": count}

@app.get("/api/n_alerts_w")
async def n_alerts_w(db: Session = Depends(get_db)):
    """Get the number of reports divided by 7"""
    count = db.query(Reports).count()
    value = count / 7 if count > 0 else 0
    value = round(value, 2)
    return {"value": value}


# ===================== PUSH NOTIFICATION ENDPOINTS =====================

@app.get("/api/vapid_public_key")
async def get_vapid_public_key():
    """Return the VAPID public key for push notifications"""
    public_key_path = os.environ.get('VAPID_PUBLIC_PATH', '/app/vapid_public.txt')
    try:
        with open(public_key_path, 'r') as f:
            public_key = f.read().strip()
        return {"public_key": public_key}
    except FileNotFoundError:
        return JSONResponse(
            status_code=500,
            content={"error": f"VAPID public key file not found at {public_key_path}"}
        )


@app.post("/api/subscribe")
async def subscribe_to_push(subscription_data: dict, db: Session = Depends(get_db)):
    """Save a push notification subscription"""
    try:
        endpoint = subscription_data.get("endpoint")
        keys = subscription_data.get("keys", {})
        p256dh = keys.get("p256dh")
        auth = keys.get("auth")

        if not all([endpoint, p256dh, auth]):
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid subscription data"}
            )

        # Check if subscription already exists
        existing = db.query(PushSubscription).filter(
            PushSubscription.endpoint == endpoint
        ).first()

        if existing:
            # Update existing subscription
            existing.p256dh = p256dh
            existing.auth = auth
        else:
            # Create new subscription
            new_subscription = PushSubscription(
                endpoint=endpoint,
                p256dh=p256dh,
                auth=auth
            )
            db.add(new_subscription)

        db.commit()
        return {"status": "success", "message": "Subscription saved"}

    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.post("/api/send_notification")
async def send_push_notification(
    notification_data: dict = None,
    db: Session = Depends(get_db)
):
    """Send push notification to all subscribed users"""
    try:
        from pywebpush import webpush, WebPushException
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        # Read VAPID private key from file path specified by env var
        vapid_private_path = os.environ.get('VAPID_PRIVATE_PATH', '/app/vapid_private.pem')
        try:
            with open(vapid_private_path, 'rb') as f:
                private_key_pem = f.read()

            # Load the private key to validate it
            from cryptography.hazmat.primitives.asymmetric import ec
            private_key = serialization.load_pem_private_key(
                private_key_pem,
                password=None,
                backend=default_backend()
            )

            # Use the file path directly for pywebpush to avoid encoding/decoding issues
            vapid_private_key = vapid_private_path

        except FileNotFoundError:
            return JSONResponse(
                status_code=500,
                content={"error": f"VAPID private key file not found at {vapid_private_path}"}
            )
        except Exception as e:
            print(f"Error loading VAPID private key: {e}")
            return JSONResponse(
                status_code=500,
                content={"error": f"Failed to load VAPID private key: {str(e)}"}
            )
        
        vapid_claims = {"sub": os.getenv("VAPID_CLAIM_EMAIL", "mailto:admin@example.com")}

        # Get all subscriptions
        subscriptions = db.query(PushSubscription).all()

        if not subscriptions:
            return {"status": "success", "message": "No subscriptions found", "sent": 0}

        # Default notification data
        if notification_data is None:
            notification_data = {}
        
        payload = json.dumps({
            "title": notification_data.get("title", "New Report Available"),
            "body": notification_data.get("body", "A new analysis report has been generated."),
            "url": notification_data.get("url", "/executive")
        })

        sent_count = 0
        failed_endpoints = []

        # Send to all subscriptions
        for subscription in subscriptions:
            try:
                subscription_info = {
                    "endpoint": subscription.endpoint,
                    "keys": {
                        "p256dh": subscription.p256dh,
                        "auth": subscription.auth
                    }
                }

                webpush(
                    subscription_info=subscription_info,
                    data=payload,
                    vapid_private_key=vapid_private_key,
                    vapid_claims=vapid_claims
                )
                sent_count += 1

            except WebPushException as e:
                print(f"Failed to send notification to {subscription.endpoint}: {e}")
                # If subscription is invalid (410 Gone or 404), remove it
                if e.response and e.response.status_code in [410, 404]:
                    failed_endpoints.append(subscription.endpoint)

        # Remove failed subscriptions
        if failed_endpoints:
            db.query(PushSubscription).filter(
                PushSubscription.endpoint.in_(failed_endpoints)
            ).delete(synchronize_session=False)
            db.commit()

        return {
            "status": "success",
            "message": f"Notifications sent to {sent_count} subscribers",
            "sent": sent_count,
            "removed": len(failed_endpoints)
        }

    except Exception as e:
        print(f"Error sending notifications: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
