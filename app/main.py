from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.logging_utils import log
from app.pipeline import run_intraday

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
    auth(x_api_key)
    log("admin_refresh_called")
    run_intraday()
    return {"ok": True}
