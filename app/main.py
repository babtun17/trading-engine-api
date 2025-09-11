from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.logging_utils import log
from app.storage import write_metrics
import time

app = FastAPI(title="Trading Engine API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def auth(x_api_key: str | None):
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

@app.get("/health")
def health():
    ok = bool(settings.SUPABASE_URL and settings.SUPABASE_SERVICE_KEY)
    return {"status": "ok" if ok else "degraded", "supabase": ok, "version": "prod-1.0"}

@app.post("/admin/refresh")
def admin_refresh(x_api_key: str | None = Header(None)):
    # No imports of pipeline/model here to keep web image slim.
    auth(x_api_key)
    # Write a “refresh requested” marker to Supabase that you can see in logs.
    write_metrics([("admin_refresh_requested", int(time.time()), 1.0)])
    log("admin_refresh_called")
    return {"ok": True, "note": "Cron runs handle training; web stays slim."}
