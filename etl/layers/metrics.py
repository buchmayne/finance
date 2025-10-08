# import pandas as pd
# import numpy as np
# from etl.database import get_db


# ## Create Tables in DB for go to metrics
# def query_marts_spending() -> pd.DataFrame:
#     marts_spending_query = """SELECT * FROM marts_spending"""
    
#     spending = (
#         pd.read_sql(marts_spending_query, get_db().connection())
#         .rename(columns=lambda c: c.lower())
#     )

#     return spending
    

# def create_average_monthly_spending_tbl() -> None:
#     """Savings (transfers to/from brokerage)"""
#     db = get_db()
#     try:
#         (
#             query_marts_spending()
#             .groupby([''])
#             .to_sql(
#                 'metrics_average_monthly_spending', 
#                 db.connection(), 
#                 if_exists='replace', 
#                 index=False
#             )
#         )
#         db.commit()
#         print(f"✅ Created metrics_average_monthly_spending table")
#     finally:
#         db.close()
