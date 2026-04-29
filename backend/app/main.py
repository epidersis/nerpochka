from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.analytics import router as analytics_router

app = FastAPI(title="Budget Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

app.include_router(
    analytics_router, prefix="/api/analytics", tags=["analytics"])


@app.get("/health")
def health():
    return {"status": "ok"}
