import numpy as np
import pandas as pd
from app.validation import validate_signals_dataframe, ValidationError
from app.constants import (
    MAX_POSITION_SIZE, MIN_PROBABILITY, CRYPTO_CAP, TARGET_DAILY_VOL,
    FEE_BPS, SLIP_BPS_BASE, SIGNAL_THRESHOLD, STRENGTH_THRESHOLD
)

def _clip(x, lo, hi): return max(lo, min(hi, x))

def size_positions_and_apply_costs(signals_df: pd.DataFrame, crypto_cap: float = CRYPTO_CAP,
                                   target_daily_vol: float = TARGET_DAILY_VOL,
                                   fee_bps: float = FEE_BPS, slip_bps_base: float = SLIP_BPS_BASE) -> pd.DataFrame:
    """
    Vol-targeted sizing:
      w_i = target_vol / (vol_est_i * sqrt(N_eff))  then normalized to leverage cap upstream if needed.
    Costs:
      Reduce effective size a bit for less liquid / higher ATR names (simple haircut).
    Crypto cap:
      Sum of |w| for crypto <= crypto_cap.
    """
    # Validate input data
    try:
        df = validate_signals_dataframe(signals_df.copy())
    except ValidationError as e:
        raise ValidationError(f"Invalid signals data: {e}")
    
    # Validate parameters
    if not 0 < crypto_cap <= 1.0:
        raise ValidationError("Crypto cap must be between 0 and 1")
    if not 0 < target_daily_vol <= 1.0:
        raise ValidationError("Target daily vol must be between 0 and 1")

    # 1) Base desired weight from probability -> convert to signed alpha strength
    #    Map prob in [0.5, 0.7+] to strength in [0, 1]
    strength = np.clip((df["prob"] - STRENGTH_THRESHOLD) / (0.2), 0, 1)
    side = np.where(df["signal"] == "long", 1.0, 0.0)  # 'flat' => 0
    desired = side * strength  # simple long-only mapping

    # 2) Vol estimate: use ATR% if available; fallback to 2% daily vol
    vol_est = df.get("atr_pct", pd.Series(0.02, index=df.index)).fillna(0.02)
    inv_vol = 1.0 / np.maximum(vol_est, 1e-4)

    # 3) Scale to target portfolio vol; naive N_eff ~ sum(desired>0)
    n_eff = max(1, int((desired > 0).sum()))
    base_scale = target_daily_vol / (np.average(vol_est[desired > 0]) if (desired > 0).any() else 0.02)
    w_raw = desired * inv_vol
    if (w_raw > 0).any():
        w_raw = w_raw / w_raw[desired > 0].sum()  # normalize sleeve to 1
    w = w_raw * base_scale

    # 4) Execution cost haircut (fee + slippage) — reduce position for costly names
    cost_bps = fee_bps + slip_bps_base
    cost_penalty = np.clip(1.0 - cost_bps / 10000.0, 0.9, 1.0)  # mild haircut
    w = w * cost_penalty

    df["size"] = w

    # 5) Crypto sleeve cap
    is_crypto = df["ticker"].str.contains("-USD")
    total_crypto = df.loc[is_crypto, "size"].abs().sum()
    if total_crypto > crypto_cap and total_crypto > 0:
        df.loc[is_crypto, "size"] *= (crypto_cap / total_crypto)

    # 6) Stop-loss / take-profit (optional soft guards as size dampeners)
    #    If prob is marginal (<threshold), dampen size further; if strong (>0.7), allow full size
    damp = np.where(df["prob"] < SIGNAL_THRESHOLD, 0.5, 1.0)
    df["size"] *= damp

    # Clamp to reasonable bounds
    df["size"] = df["size"].clip(lower=0.0, upper=MAX_POSITION_SIZE)
    return df
