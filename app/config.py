from pydantic import BaseModel
import os
class Settings(BaseModel):
    HOST: str = '0.0.0.0'
    PORT: int = 10000
    API_KEY: str = os.getenv('API_KEY','change-me')
    CORS_ORIGINS: list[str] = os.getenv('CORS_ORIGINS','*').split(',')
    DB_PATH: str = os.getenv('DB_PATH','db.sqlite')
settings = Settings()
