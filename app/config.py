from pydantic import BaseModel
import os

def _list_env(name: str, default: str = "*") -> list[str]:
    return os.getenv(name, default).split(",")

class Settings(BaseModel):
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 10000
    API_KEY: str = os.getenv("API_KEY", "change-me")
    CORS_ORIGINS: list[str] = _list_env("CORS_ORIGINS", "*")

    # Supabase (Service Role; never exposed client-side)
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_KEY: str = os.getenv("SUPABASE_SERVICE_KEY", "")

    # Universe & risk defaults (pipelines)
    MAX_US: int = int(os.getenv("MAX_US", 60))
    MAX_UK: int = int(os.getenv("MAX_UK", 20))
    MAX_CRYPTO: int = int(os.getenv("MAX_CRYPTO", 5))
    CRYPTO_CAP_WEIGHT: float = float(os.getenv("CRYPTO_CAP_WEIGHT", 0.05))

    # Costs & sizing
    FEE_BPS: float = float(os.getenv("FEE_BPS", 1.0))
    SLIP_BPS_BASE: float = float(os.getenv("SLIP_BPS_BASE", 3.0))
    SLIP_BPS_K_ATR: float = float(os.getenv("SLIP_BPS_K_ATR", 50.0))
    TARGET_DAILY_VOL: float = float(os.getenv("TARGET_DAILY_VOL", 0.01))
    MAX_LEVERAGE: float = float(os.getenv("MAX_LEVERAGE", 1.5))

settings = Settings()
