import pandas as pd
import numpy as np
from etl.database import get_db

def _prepare_bank_account_tx_for_union(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .loc[~df['category'].isin(['TRANSFER_BETWEEN_CHASE_ACCOUNTS', 'CREDIT_CARD_PAYMENT'])]
        .drop(['balance', 'source_file', 'imported_at'], axis=1)
        .rename(columns={'account': 'account_or_card_number'})
        .assign(source='bank_account')
    )

def _prepare_credit_card_tx_for_union(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .loc[~df['category'].isin(['CREDIT_CARD_PAYMENT'])]
        .drop(['chase_category', 'source_file', 'imported_at'], axis=1)
        .rename(columns={'card_number': 'account_or_card_number'})
        .assign(source='credit_card')
    )

def create_unified_transactions() -> None:
    staging_bank_acc_tx_query = """SELECT * FROM staging_bank_account_transactions"""
    staging_cc_tx_query = """SELECT * FROM staging_credit_card_transactions"""
    
    bank_account = (
        pd.read_sql(staging_bank_acc_tx_query, get_db().connection())
        .pipe(_prepare_bank_account_tx_for_union)
    )

    credit_card = (
        pd.read_sql(staging_cc_tx_query, get_db().connection())
        .pipe(_prepare_credit_card_tx_for_union)
    )
    
    unified_transactions = pd.concat([credit_card, bank_account], axis=0)

    return unified_transactions

if __name__ == "__main__":
    df = create_unified_transactions()

    monthly_income = (
        df
        .loc[df['category'] == 'SALARY']
        .groupby(['year', 'month'], as_index=False)
        .agg(salary=('amount', 'sum'))
    )

    monthly_cash_flow = (
        df
        .groupby(['year', 'month'], as_index=False)
        .agg(cash_flow=('amount', 'sum'))
        .assign(cumulative_cash_flow=lambda df_: df_['cash_flow'].cumsum())
    )

    monthly_spending_by_category = (
        df
        .groupby(['year', 'month', 'category'], as_index=False)
        .agg(total_spend=('amount', 'sum'))
        .loc[lambda df_: (df_['year'] == 2024) & (df_['month'] == 10)]
        .sort_values('total_spend', ascending=False)
        .assign(net_cash_flow=lambda df_: df_['total_spend'].sum())
    )

    print(monthly_income)
    print('\n\n')
    print(monthly_cash_flow)
    print('\n\n')
    print(monthly_spending_by_category)
