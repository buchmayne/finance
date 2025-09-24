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

def create_savings_table(df: pd.DataFrame) -> pd.DataFrame:
    """Savings are defined as transfers to/from investment accounts"""
    return (
        df
        .loc[df['category'].isin(['TRANSFER_TO_BROKERAGE', 'TRANSFER_FROM_BROKERAGE'])]
        .assign(amount=lambda df_: df_['amount'] * -1) # transfer to brokerage is a deduction from bank account but increase in savings
    )

def drop_savings_from_tx_tbl(df: pd.DataFrame) -> pd.DataFrame:
    """Remove savings from transaction table to better identify patterns in spending and earning"""
    return df.loc[~df['category'].isin(['TRANSFER_TO_BROKERAGE', 'TRANSFER_FROM_BROKERAGE'])]

def assign_categories_to_concepts(df: pd.DataFrame) -> pd.DataFrame:
    """Group categories together into concepts for easier analysis"""
    subscriptions = [
        'PODCAST_SUBSCRIPTION', 
        'HBO_SUBSCRIPTION', 
        'SPOTIFY_MEMBERSHIP', 
        'APPLE_CLOUD_STORAGE', 
        'AI_SUBSCRIPTION', 
        'PARAMOUNT_SUBSCRIPTION', 
        'CHESS_SUBSCRIPTION', 
        'AMAZON_PRIME', 
        'BLAZER_VISION_SUBSCRIPTION', 
        'PEACOCK_SUBSCRIPTION',
        'VIDEO_GAMES'
    ]

    return (
        df
        .assign(concept=np.select(
            [
                df['category'].isin(['MORTGAGE_PAYMENT', 'HOA_PAYMENT']),
                df['category'].isin(['JENNA_WEDDING_ACCT_TRANSFERS', 'WEDDING_PHOTOGRAPHER', 'WEDDING']),
                df['category'].isin(subscriptions),
                df['category'].isin(['SALARY', 'CASH_DEPOSIT', 'TAX_REFUND', 'ACCOUNT_INTEREST', 'CASH_WITHDRAWL', 'PORTLAND_ARTS_TAX', 'FILING_TAXES']),
                df['category'].isin(['CAR_INSURANCE', 'DIAMOND_INSURANCE', 'COBRA_PAYMENTS']),
                df['category'].isin(['CELL_PHONE_BILL', 'COMCAST', 'PGE', 'HAIRCUT']),
                df['category'].isin(['FAST_FOOD', 'OVATION_WEEKEND', 'EATING_OUT_NBHD_LUNCH', 'OVATION_WEEKDAY', 'EATING_OUT', 'DOMINOS', 'OTHER_COFFEE_SHOPS', 'NBHD_BARS']),
                df['category'] == 'GROCERIES',
                df['category'].isin(['RIDESHARE', 'TRAVEL_LODGING', 'PARKING', 'FLIGHTS', 'OTHER_TRANSPORTATION', 'PASSPORT_RENEWAL']),
                df['category'].isin(['VOD_AMAZON', 'MOVIES']),
                df['category'].isin(['PHYSICAL_MEDIA', 'POWELLS', 'MTG']),
                df['category'].isin(['CONCERTS', 'RODEO', 'MODA_CENTER']),
                df['category'].isin(['LIQUOR_STORE']),
                df['category'].isin(['GYM_MEMBERSHIP', 'INDOOR_SOCCER', 'SURFING']),
                df['category'].isin(['CLOTHES', 'ARSENAL', 'DRY_CLEANING']),
                df['category'].isin(['GAS', 'CAR_MAINTENANCE']),
                df['category'].isin(['VENMO_PAYMENT', 'VENMO_CASHOUT']),
                df['category'].isin(['HOSTING_SOFTWARE_PROJECTS', 'COMPUTERS_TECHNOLOGY_HARDWARE']),
                df['category'] == 'AMAZON_PURCHASE'
            ],
            [
                'HOUSING',
                'WEDDING',
                'ENTERTAINMENT_SUBSCRIPTIONS',
                'INCOME',
                'INSURANCE',
                'UTILITIES',
                'EATING_OUT',
                'GROCERIES',
                'TRAVEL',
                'MOVIES',
                'HOBBY_PHYSICAL_MEDIA',
                'CONCERTS_AND_SPORTING_EVENTS',
                'HOBBY_COCKTAILS',
                'HOBBY_SPORTS',
                'CLOTHES',
                'CAR',
                'VENMO',
                'HOBBY_TECH',
                'AMAZON_SPENDING'
            ],
            default='OTHER'
        ))
    )

if __name__ == "__main__":
    
    base_df = create_unified_transactions()
    savings_df = create_savings_table(base_df)

    # drop savings from transactions table
    tx_df = drop_savings_from_tx_tbl(base_df)

    monthly_income = (
        tx_df
        .loc[tx_df['category'] == 'SALARY']
        .groupby(['year', 'month'], as_index=False)
        .agg(salary=('amount', 'sum'))
    )

    monthly_cash_flow = (
        tx_df
        .groupby(['year', 'month'], as_index=False)
        .agg(cash_flow=('amount', 'sum'))
        .assign(cumulative_cash_flow=lambda df_: df_['cash_flow'].cumsum())
    )

    monthly_spending_by_category = (
        tx_df
        .groupby(['year', 'month', 'category'], as_index=False)
        .agg(total_spend=('amount', 'sum'))
    )

    monthly_savings = (
        savings_df
        .groupby(['year', 'month'], as_index=False)
        .agg(savings=('amount', 'sum'))
        .assign(cumulative_savings=lambda df_: df_['savings'].cumsum())
    )

    annual_spend_by_concept = (
        assign_categories_to_concepts(tx_df)
        .groupby(['concept', 'year'], as_index=False)
        .agg(concept_spending=('amount', 'sum'))
    )

    print(annual_spend_by_concept)