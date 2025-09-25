from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from sqlalchemy import create_engine, text
from etl.config import Settings
import uvicorn
import json
import pandas as pd

app = FastAPI(title="Personal Finance Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

marts_tables = [
    "marts_monthly_salary",
    "marts_monthly_cash_flow",
    "marts_monthly_spending_by_category",
    "marts_monthly_spending_by_meta_category",
    "marts_unified_transactions",
    "marts_monthly_savings"
]

# Database connection
engine = create_engine(Settings.database_url)

def execute_query(query: str) -> List[Dict[str, Any]]:
    """Execute SQL query and return results as list of dictionaries"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failled: {str(e)}")


@app.get("/api/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/transactions/monthly_cash_flow")
def get_monthly_cash_flow(
    start_year: Optional[int] = Query(None, description="Start year (2023 first year in database)"),
    end_year: Optional[int] = Query(None, description="End year"),
    months: Optional[int] = Query(12, description="Number of recent months if no year range specified")
) -> dict:
    """
    Return monthly cash flow data from marts table
    """
    if start_year and end_year:
        query = """
        SELECT 
            year, month, year || '-' || PRINTF('%02d', month) as date_key, cash_flow, cumulative_cash_flow
        FROM marts_monthly_cash_flow
        WHERE year BETWEEN :start_year AND :end_year
        ORDER BY year, month
        """
        result = execute_query(query.replace(':start_year', str(start_year)).replace(':end_year', str(end_year)))
    else:
        query = f"""
        SELECT year, month, year || '-' || PRINTF('%02d', month) as date_key, cash_flow, cumulative_cash_flow
        FROM marts_monthly_cash_flow
        ORDER BY year DESC, month DESC
        LIMIT {months}
        """
        result = execute_query(query)
        result.reverse()
    
    return {
        "data": result,
        "metadata": {
            "count": len(result),
            "type": "cash_flow",
            "currency": "USD"
        }
    }

@app.get("/api/transactions/by-category")  
def get_spending_by_category(
    period: str = Query("last_12_months", description="Time period: last_12_months, ytd, or specific year"),
    level: str = Query("category", description="Aggregation level: category or meta_category"),
    year: Optional[int] = Query(None, description="Specific year if period='year'")
):
    """
    Get spending breakdown by category.
    Returns data suitable for bar charts showing category spending.
    """
    
    # Choose table based on aggregation level
    grouping_col = "meta_category" if level == "meta_category" else "category" 
    
    if period == "ytd":
        current_year = datetime.now().year
        where_clause = f"WHERE year = {current_year} AND meta_category != 'INCOME'"
    elif period == "year" and year:
        where_clause = f"WHERE year = {year} AND meta_category != 'INCOME'"
    else:  # last_12_months (default)
        where_clause = """
        WHERE (year * 12 + month) >= 
              ((SELECT MAX(year * 12 + month) FROM marts_unified_transactions) - 11)
              AND meta_category != 'INCOME'
        """
    
    query = f"""
    SELECT 
        year,
        month,
        {grouping_col} as category,
        SUM(amount) * -1 as total_spending
    FROM marts_unified_transactions
    {where_clause}
    GROUP BY year, month, {grouping_col}
    ORDER BY year, month
    """
    
    result = execute_query(query)
    
    return {
        "data": result,
        "metadata": {
            "count": len(result),
            "period": period,
            "level": level,
            "total": sum(item["total_spending"] for item in result)
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)