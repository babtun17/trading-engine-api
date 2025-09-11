import pandas as pd
def make_panel_with_augments(panel: pd.DataFrame, use_finbert: bool = False) -> pd.DataFrame:
    df = panel.copy()
    # Minimal illustrative features
    df["rsi14"] = 50.0
    df["atr_pct"] = 0.02
    return df
