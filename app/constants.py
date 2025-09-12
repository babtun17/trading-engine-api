"""
Constants and configuration values for the trading engine
"""

# Trading Limits
MAX_POSITION_SIZE = 0.15  # 15% max per position
MIN_PROBABILITY = 0.6     # Minimum probability for trading
MAX_LEVERAGE = 1.0        # Maximum leverage
CRYPTO_CAP = 0.05         # 5% max crypto allocation

# Risk Management
TARGET_DAILY_VOL = 0.01   # 1% target daily volatility
FEE_BPS = 1.0             # 1 basis point fee
SLIP_BPS_BASE = 3.0       # 3 basis points base slippage

# Data Processing
DEFAULT_CHUNK_SIZE = 4    # Ticker download chunk size
DEFAULT_PAUSE = 1.0       # Pause between downloads
MAX_RETRIES = 3           # Maximum retry attempts

# Model Parameters
RSI_PERIOD = 14           # RSI calculation period
ATR_PERIOD = 14           # ATR calculation period
MOMENTUM_PERIODS = [10, 20]  # Momentum calculation periods
VOLATILITY_PERIODS = [20, 60]  # Volatility calculation periods

# Signal Thresholds
SIGNAL_THRESHOLD = 0.6    # Probability threshold for long signals
STRENGTH_THRESHOLD = 0.5  # Base strength threshold

# Order Management
ORDER_DOLLAR_STEP = 1000  # Default order size step
MIN_ORDER_SIZE = 1        # Minimum order quantity

# Data Validation
MAX_TICKER_LENGTH = 10    # Maximum ticker symbol length
MIN_TICKER_LENGTH = 1     # Minimum ticker symbol length

# Time Periods
DATA_PERIOD = "2y"        # Default data period
DATA_INTERVAL = "1d"      # Default data interval
NEWS_CUTOFF_DAYS = 14     # Days to look back for news sentiment

# Error Handling
MAX_FAILED_DOWNLOADS = 5  # Max failed downloads before giving up
TIMEOUT_SECONDS = 10      # Request timeout in seconds
