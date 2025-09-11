import json, sys, time
def log(event: str, **kw):
    payload = {"t": int(time.time()), "event": event, **kw}
    print(json.dumps(payload, ensure_ascii=False), file=sys.stdout, flush=True)
