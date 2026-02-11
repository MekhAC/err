from fastapi import FastAPI, Query
from datetime import datetime, timedelta

from info import LogStore
from parse import Parser

app = FastAPI(title="Log Processing & Analytics Service")

log_store = LogStore()
parser = Parser(log_store)

@app.on_event("startup")
def load_logs():
    parser.parse_logs()


@app.get("/levels")
 async def by_level():
    return dict(log_store.level_count)


@app.get("/services")
async def by_service():
    return dict(log_store.service_count)


@app.get("/top-services")
async def top_services(n: int = Query(3, ge=1, le=20)):
    return log_store.service_count.most_common(n)


@app.get("/errors")
async def recent_errors(minutes: int = Query(10)):
    cutoff = datetime.now() - timedelta(minutes=minutes)

    # sliding window eviction
    while log_store.error_logs and log_store.error_logs[0][0] < cutoff:
        log_store.error_logs.popleft()

    return [
        {
            "timestamp": ts.isoformat(),
            "service": service,
            "message": message
        }
        for ts, service, message in log_store.error_logs
    ]
