from fastapi import FastAPI
from app.api.analytics import router as analytics_router

app = FastAPI(title="Budget Analytics API")

app.include_router(
    analytics_router, prefix="/api/analytics", tags=["analytics"])


@app.get("/health")
def health():
    return {"status": "ok"}
