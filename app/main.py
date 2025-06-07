from fastapi import FastAPI
from app.database import Base, engine
from app import models

app = FastAPI(
    title="CryptoPortfolioTracker API",
    description="API to track your crypto portfolio and transactions.",
    version="0.1.0"
)

# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/health")
def health_check():
    return {"status": "ok"}
