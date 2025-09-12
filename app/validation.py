"""
Input validation for trading engine
"""
import re
from typing import List, Union
import pandas as pd

class ValidationError(Exception):
    """Raised when input validation fails"""
    pass

def validate_ticker(ticker: str) -> bool:
    """
    Validate ticker format:
    - US stocks: AAPL, MSFT (1-5 letters)
    - UK stocks: AZN.L, BP.L (1-5 letters + .L)
    - Crypto: BTC-USD, ETH-USD (3-5 letters + -USD)
    """
    if not isinstance(ticker, str):
        return False
    
    pattern = r'^[A-Z]{1,5}(\.L)?(-USD)?$'
    return bool(re.match(pattern, ticker))

def validate_probability(prob: Union[float, int]) -> bool:
    """Validate probability is between 0 and 1"""
    try:
        prob_float = float(prob)
        return 0.0 <= prob_float <= 1.0
    except (ValueError, TypeError):
        return False

def validate_position_size(size: Union[float, int]) -> bool:
    """Validate position size is reasonable (0-20%)"""
    try:
        size_float = float(size)
        return 0.0 <= size_float <= 0.2  # Max 20% per position
    except (ValueError, TypeError):
        return False

def validate_ticker_list(tickers: List[str]) -> List[str]:
    """Validate list of tickers, return valid ones"""
    if not isinstance(tickers, list):
        raise ValidationError("Tickers must be a list")
    
    valid_tickers = []
    for ticker in tickers:
        if validate_ticker(ticker):
            valid_tickers.append(ticker)
        else:
            print(f"Warning: Invalid ticker format: {ticker}")
    
    if not valid_tickers:
        raise ValidationError("No valid tickers provided")
    
    return valid_tickers

def validate_signals_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Validate signals DataFrame has required columns and valid data"""
    required_columns = ['ticker', 'prob', 'signal', 'size']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValidationError(f"Missing required columns: {missing_columns}")
    
    # Validate tickers
    invalid_tickers = df[~df['ticker'].apply(validate_ticker)]
    if not invalid_tickers.empty:
        raise ValidationError(f"Invalid tickers: {invalid_tickers['ticker'].tolist()}")
    
    # Validate probabilities
    invalid_probs = df[~df['prob'].apply(validate_probability)]
    if not invalid_probs.empty:
        raise ValidationError(f"Invalid probabilities: {invalid_probs['prob'].tolist()}")
    
    # Validate sizes
    invalid_sizes = df[~df['size'].apply(validate_position_size)]
    if not invalid_sizes.empty:
        raise ValidationError(f"Invalid position sizes: {invalid_sizes['size'].tolist()}")
    
    return df

def validate_signal_threshold(threshold: Union[float, int]) -> float:
    """Validate and normalize signal threshold"""
    try:
        threshold_float = float(threshold)
        if not 0.0 <= threshold_float <= 1.0:
            raise ValidationError("Signal threshold must be between 0 and 1")
        return threshold_float
    except (ValueError, TypeError):
        raise ValidationError("Signal threshold must be a number")
