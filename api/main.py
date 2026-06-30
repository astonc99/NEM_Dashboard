import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import prices, generation, analytics

logger = logging.getLogger(__name__)

_sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="startup-sync")

# Shared sync state — read by /api/sync/status
_sync_state: dict = {"status": "idle", "prices": None, "scada": None}


def _run_startup_sync():
    global _sync_state
    _sync_state["status"] = "running"
    try:
        from etl.auto_sync import sync_prices, sync_scada
        logger.info("Startup sync — prices...")
        r = sync_prices(max_months=12)
        _sync_state["prices"] = r
        logger.info(f"Startup sync prices complete: {r.get('months_synced')}")

        logger.info("Startup sync — SCADA...")
        r = sync_scada(max_months=12)
        _sync_state["scada"] = r
        logger.info(f"Startup sync SCADA complete: {r.get('months_synced')}")

        _sync_state["status"] = "done"
    except Exception as e:
        logger.exception("Startup sync failed")
        _sync_state["status"] = "error"
        _sync_state["error"] = str(e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import os
    if os.environ.get("DEMO_MODE") != "true":
        loop = asyncio.get_event_loop()
        loop.run_in_executor(_sync_executor, _run_startup_sync)
    yield


app = FastAPI(title="NEM Dashboard API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(prices.router,     prefix="/api/prices",     tags=["prices"])
app.include_router(generation.router, prefix="/api/generation", tags=["generation"])
app.include_router(analytics.router,  prefix="/api/analytics",  tags=["analytics"])


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/sync/status")
def sync_status():
    return _sync_state
