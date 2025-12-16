import time

def log(event, **fields):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    data = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[{ts}] {event} {data}")