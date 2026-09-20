"""FastAPI app entry point.

Local dev:   uvicorn app.api.main:app --reload --port 8000
Lambda:      via Mangum in app.api.mangum_handler
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import articles, candidates, meta, sources, timeseries

app = FastAPI(
    title="mediaElection27 API",
    description="French media RSS analytics for the 2027 presidential election.",
    version="0.1.0",
)

# CORS: open during dev so the Vite frontend on :5173 can call us. Tighten
# this to specific origins before any public deploy.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(meta.router)
app.include_router(candidates.router)
app.include_router(sources.router)
app.include_router(timeseries.router)
app.include_router(articles.router)
