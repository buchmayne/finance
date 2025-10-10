from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm.session import Session
from etl.config import Settings
from etl.database import get_db
import uvicorn
import json
import pandas as pd
from metrics import (
    calculate_average_monthly_spending_by_meta_category,
    calculate_monthly_budget_history,
    calculate_average_monthly_budget,
)

app = FastAPI(title="Personal Finance Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/api/metrics/spending-by-category")
def get_spending_by_category(
    period: str = "full_history",
    include_wedding: bool = True,
    db: Session = Depends(get_db),
):
    """
    Spending by category
    """
    df = calculate_average_monthly_spending_by_meta_category(
        db, period, include_wedding
    )
    # Convert to JSON-serializable format
    return df.to_dict(orient="records")


@app.get("/api/metrics/monthly-budget-history")
def get_monthly_budget_history(
    period: str = "full_history",
    include_wedding: bool = False,
    db: Session = Depends(get_db),
):
    """
    Spending and Earning over time
    """
    df = calculate_monthly_budget_history(db, period, include_wedding)
    return df.to_dict(orient="records")


@app.get("/api/metrics/average-monthly-budget")
def get_average_monthly_budget(
    period: str = "full_history",
    include_wedding: bool = False,
    db: Session = Depends(get_db),
):
    """
    Average monthly budget
    """
    df = calculate_average_monthly_budget(db, period, include_wedding)
    return df.to_dict(orient="records")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
