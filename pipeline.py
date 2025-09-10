def run_daily(): print("daily stub ok")
def run_intraday(): print("intraday stub ok")
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv)>1 else "help"
    if cmd == "daily": run_daily()
    elif cmd == "intraday": run_intraday()
    else: print("Usage: python -m app.pipeline [daily|intraday]")
