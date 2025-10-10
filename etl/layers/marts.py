import pandas as pd
import numpy as np
from etl.database import get_db


# Parameters
INCOME_CATEGORIES = ['SALARY', 'CASH_DEPOSIT', 'TAX_REFUND', 'ACCOUNT_INTEREST', 'PORTLAND_ARTS_TAX', 'FILING_TAXES', 'VENMO_CASHOUT']
SAVINGS_CATEGORIES = ['TRANSFER_TO_BROKERAGE', 'TRANSFER_FROM_BROKERAGE']


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

def create_unified_transactions() -> pd.DataFrame:
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

def subset_transactions_on_savings(df: pd.DataFrame) -> pd.DataFrame:
    """Savings are defined as transfers to/from investment accounts"""
    return (
        df
        .loc[df['category'].isin(SAVINGS_CATEGORIES)]
        .assign(amount=lambda df_: df_['amount'] * -1) # transfer to brokerage is a deduction from bank account but increase in savings
    )

def drop_savings_from_tx_tbl(df: pd.DataFrame) -> pd.DataFrame:
    """Remove savings from transaction table to better identify patterns in spending and earning"""
    return df.loc[~df['category'].isin(SAVINGS_CATEGORIES)]

def drop_income_from_tx_tbl(df: pd.DataFrame) -> pd.DataFrame:
    """Remove income from transaction table to better identify patterns in spending"""
    return df.loc[~df['category'].isin(INCOME_CATEGORIES)]

def assign_categories_to_meta_categories(df: pd.DataFrame) -> pd.DataFrame:
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
        .assign(meta_category=np.select(
            [
                df['category'].isin(['MORTGAGE_PAYMENT', 'HOA_PAYMENT']),
                df['category'].isin(['JENNA_WEDDING_ACCT_TRANSFERS', 'WEDDING_PHOTOGRAPHER', 'WEDDING', 'CASH_WITHDRAWL_FOR_WEDDING']),
                df['category'].isin(subscriptions),
                df['category'].isin(['SALARY', 'CASH_DEPOSIT', 'TAX_REFUND', 'ACCOUNT_INTEREST', 'PORTLAND_ARTS_TAX', 'FILING_TAXES']),
                df['category'].isin(['CASH_WITHDRAWL']),
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
                df['category'].isin(['VENMO_PAYMENT']),
                df['category'].isin(['HOSTING_SOFTWARE_PROJECTS', 'COMPUTERS_TECHNOLOGY_HARDWARE']),
                df['category'] == 'AMAZON_PURCHASE'
            ],
            [
                'HOUSING',
                'WEDDING',
                'ENTERTAINMENT_SUBSCRIPTIONS',
                'INCOME',
                'CASH_WITHDRAWL',
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

## Create Tables in DB for cleaned Transactions, Spending, Income, Savings
def create_transactions_tbl() -> None:
    """Clean transactions data with meta categories"""
    db = get_db()
    try:
        (
            create_unified_transactions()
            .pipe(drop_savings_from_tx_tbl)
            .pipe(assign_categories_to_meta_categories)
            .to_sql(
                'marts_transactions',
                db.connection(),
                if_exists='replace',
                index=False
            )
        )
        db.commit()
        print(f"✅ Created marts_transactions table")
    finally:
        db.close()


def create_spending_tbl() -> None:
    """
    Extract all spending by removing income and savings from transactions.
    """
    db = get_db()
    try:
        (
            create_unified_transactions()
            .pipe(drop_savings_from_tx_tbl)
            .pipe(drop_income_from_tx_tbl)
            .pipe(assign_categories_to_meta_categories)
            # spending is recorded as a deduction but for reporting we want it to be a positive value
            .assign(amount=lambda df_: df_['amount'] * -1) 
            .to_sql(
                'marts_spending',
                db.connection(),
                if_exists='replace',
                index=False
            )
        )
        db.commit()
        print(f"✅ Created marts_spending table")
    finally:
        db.close()


def create_income_tbl() -> None:
    """
    Extract all earnings (Salary, Venmo cash outs, tax refunds, cash deposits, etc.) from transactions table into
    separate income table
    """
    db = get_db()
    try:
        (
            create_unified_transactions()
            .pipe(drop_savings_from_tx_tbl)
            .loc[lambda df_: df_['category'].isin(INCOME_CATEGORIES)]
            .to_sql(
                'marts_income',
                db.connection(),
                if_exists='replace',
                index=False
            )
        )
        db.commit()
        print(f"✅ Created marts_income table")
    finally:
        db.close()
    


def create_savings_tbl() -> None:
    """Savings (transfers to/from brokerage)"""
    db = get_db()
    try:
        (
            create_unified_transactions()
            .pipe(subset_transactions_on_savings)
            .to_sql(
                'marts_savings', 
                db.connection(), 
                if_exists='replace', 
                index=False
            )
        )
        db.commit()
        print(f"✅ Created marts_savings table")
    finally:
        db.close()

