from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
app = FastAPI(title='Trading Engine API', version='1.0')
app.add_middleware(CORSMiddleware, allow_origins=settings.CORS_ORIGINS, allow_credentials=True, allow_methods=['*'], allow_headers=['*'])
@app.get('/health')
def health():
    return {'status':'ok','msg':'fixed layout running'}
