from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import prices, generation, analytics

app = FastAPI(title="NEM Dashboard API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(prices.router,    prefix="/api/prices",    tags=["prices"])
app.include_router(generation.router, prefix="/api/generation", tags=["generation"])
app.include_router(analytics.router,  prefix="/api/analytics",  tags=["analytics"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
