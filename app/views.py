import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from sqlalchemy.orm.session import Session
from etl.database import get_db

JENNA_MORTGAGE_CONTRIBUTION = 850

def determine_last_X_months_from_dataset(df: pd.DataFrame, number_of_months: int) -> np.array:
    """
    Given transaction data determine the most recent X year-months from the dataset
    """
    return (
        df
        ['year_month']
        .drop_duplicates()
        .sort_values(ascending=False)
        .reset_index(drop=True)
        .iloc[:number_of_months]
        .values
    )

def include_wedding_spending(df: pd.DataFrame, include_wedding: bool = True) -> pd.DataFrame:
    """
    Wedding spending is a large outlier in terms of spending patterns and is not a repeatable expense.
    For the sake of financial planning it is useful to be able to toggle whether or not wedding expenses
    are included in the analysis dataset.
    """
    if not include_wedding:
        return df.loc[df['meta_category'] != 'WEDDING']
    else:
        return df

def subset_data_by_period(df: pd.DataFrame, period: str = "full_history") -> pd.DataFrame:
    """
    Subset dataset based on inputed time period.
    Options include year to date, last 1/3/6/12 months of data, or the full dataset.
    
    df: pd.DataFrame  = any marts table data
    period: str = 'ytd', 'last_1_months', 'last_3_months', 'last_6_months', 'last_12_months', 'full_history'
    """
    if period == 'ytd':
        data = df.loc[df['year'] == df['year'].max()]
    elif period == 'last_1_months':
        data = df.loc[df['year_month'].isin(determine_last_X_months_from_dataset(df, number_of_months=1))]
    elif period == 'last_3_months':
        data = df.loc[df['year_month'].isin(determine_last_X_months_from_dataset(df, number_of_months=3))]
    elif period == 'last_6_months':
        data = df.loc[df['year_month'].isin(determine_last_X_months_from_dataset(df, number_of_months=6))]
    elif period == 'last_12_months':
        data = df.loc[df['year_month'].isin(determine_last_X_months_from_dataset(df, number_of_months=12))]
    elif period == 'full_history':
        data = df
    else:
        raise ValueError("Invalid period parameter. period can be 'ytd', 'last_1_months', 'last_3_months', 'last_6_months', 'last_12_months', 'full_history'")
    return data


def calculate_average_monthly_spending_by_meta_category(
    db: Session,
    period: str = "full_history",
    include_wedding: bool = True
) -> pd.DataFrame:
    """
    View of average monthly spending patterns.
    """
    data = (
        pd.read_sql("""SELECT * FROM marts_spending""", db.connection())
        .pipe(subset_data_by_period, period)
        .pipe(include_wedding_spending, include_wedding)
    )
    
    monthly_data = (
        data
        .groupby(['year_month', 'meta_category'], as_index=False)
        .agg(
            total_spend=('amount', 'sum'),
            monthly_transactions=('amount', 'count'),
        )
    )

    number_of_months = monthly_data['year_month'].unique().shape[0]
    
    return (
        monthly_data
        .groupby(['meta_category'], as_index=False)
        .agg(
            total_spend=('total_spend', 'sum'),
            avg_monthly_transactions=('monthly_transactions', 'mean'),
            months_with_transactions=('total_spend', 'count')
        )
        .assign(avg_monthly_spend=lambda df_: df_['total_spend'] / number_of_months)
        .assign(
            pct_of_avg_monthly_spend=lambda df_: round(df_['avg_monthly_spend'] / df_['avg_monthly_spend'].sum(), 4) * 100,
            number_of_months_in_sample=number_of_months
        )
        .assign(spending_occurs_every_month=lambda df_: df_['number_of_months_in_sample'] == df_['months_with_transactions'])
        .sort_values('avg_monthly_spend', ascending=True)
        [[
            'meta_category',
            'number_of_months_in_sample',
            'months_with_transactions',
            'spending_occurs_every_month',
            'avg_monthly_transactions',
            'total_spend',
            'avg_monthly_spend',
            'pct_of_avg_monthly_spend',
        ]]
    )

def calculate_monthly_spending(db: Session, period: str = "full_history", include_wedding: bool = True) -> pd.DataFrame:
    return (
        pd.read_sql("""SELECT * FROM marts_spending""", db.connection())
        .pipe(subset_data_by_period, period)
        .pipe(include_wedding_spending, include_wedding)
        .groupby(['year_month'], as_index=False)
        .agg(monthly_spending=('amount', 'sum'))
    )

def calculate_monthly_saving(db: Session, period: str = "full_history") -> pd.DataFrame:
    """
    Calculate savings per month over inputed time period
    """
    return (
        pd.read_sql("""SELECT * FROM marts_savings""", db.connection())
        .pipe(subset_data_by_period, period)
        .assign(year_month=lambda df_: pd.to_datetime(df_['year_month']))
        .groupby(['year_month'])
        .agg(monthly_savings=('amount', 'sum'))
        .pipe(lambda df_: df_.reindex(pd.date_range(start=df_.index.min(), end=df_.index.max(), freq='MS'), fill_value=0))
        .reset_index(names='year_month')
        .assign(year_month=lambda df_: df_['year_month'].astype(str).str[:7]) # convert to format used elsewhere for join
    )

def calculate_average_monthly_saving(db: Session, period: str = "full_history") -> float:
    """
    For the given time period calculate average monthly savings
    """
    return float(
        calculate_monthly_saving(db, period)
        ['monthly_savings']
        .mean()
    )

def calculate_monthly_salary(db: Session, period: str = "full_history") -> pd.DataFrame:
    """
    Calculate my monthly salary income for the given time period.
    """
    return (
        pd.read_sql("""SELECT * FROM marts_income""", db.connection())
        .pipe(subset_data_by_period, period)
        .loc[lambda df_: df_['category'] == 'SALARY']
        .groupby(['year_month'], as_index=False)
        .agg(monthly_salary=('amount', 'sum'))
    )

def calculate_average_monthly_salary(db: Session, period: str = "full_history") -> float:
    """
    For the given time period calculate average monthly salary
    """
    return float(
        calculate_monthly_salary(db, period)
        ['monthly_salary']
        .mean()
    )

def calculate_average_monthly_budget(db: Session, period: str = "full_history", include_wedding: bool = True) -> pd.DataFrame:
    """
    Calculate the average spending and income per month over a given period to
    get a monthly cash flow estimate for budgeting purposes.
    """
    spending = (
        calculate_average_monthly_spending_by_meta_category(db, period, include_wedding)
        [['meta_category', 'avg_monthly_spend']]
        .rename(columns={'meta_category': 'description', 'avg_monthly_spend': 'amount'})
        .assign(
            amount=lambda df_: df_['amount'] * -1,
            category='SPENDING'
        )
    )

    income = pd.DataFrame({
        'description': ['SALARY', 'MORTGAGE_CONTRIBUTION'],
        'amount': [calculate_average_monthly_salary(db, period), JENNA_MORTGAGE_CONTRIBUTION],
        'category': ['INCOME', 'INCOME']
    }, index=[0, 1])

    return (
        pd.concat([spending, income], axis=0)
        .pipe(lambda df_: 
              pd.concat([
                  df_,
                  pd.DataFrame({
                      'description': 'CASH_FLOW',
                      'amount': df_['amount'].sum(),
                      'category': 'CASH_FLOW'
                  }, index=[0])
              ], axis=0)
        )
    )

def calculate_monthly_budget(
    db: Session,
    period: str = "full_history",
    include_wedding: bool = True
) -> pd.DataFrame:
    """
    For each month in the time period calculate total spending, income, and savings.
    """
    return (
        calculate_monthly_spending(db, period, include_wedding)
        .merge(calculate_monthly_salary(db, period), how='left')
        .merge(calculate_monthly_saving(db, period), how='left')
        .assign(cumulative_savings=lambda df_: df_['monthly_savings'].cumsum())
    )

# Visualization Functions
def plot_monthly_spending_by_category(db: Session, period: str = 'full_history', include_wedding: bool = True) -> None:
    df = calculate_average_monthly_spending_by_meta_category(db, period, include_wedding)
    
    fig, ax = plt.subplots(figsize=(10, 8))

    # Binary color: one color for every-month spending, another for sporadic
    colors = ['#8fbcbb' if every_month else '#5e81ac' 
              for every_month in df['spending_occurs_every_month']]
    
    bars = ax.barh(df['meta_category'], df['avg_monthly_spend'], color=colors)
    
    # Annotate with percentage
    for i, (spend, pct) in enumerate(zip(df['avg_monthly_spend'], df['pct_of_avg_monthly_spend'])):
        ax.text(spend + 10, i, f"{pct:.1f}%", va='center', fontsize=9)
    
    ax.set_xlabel('Avg Monthly Spend ($)')
    ax.set_title('Monthly Budget Breakdown')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    legend_elements = [
        Patch(facecolor='#8fbcbb', label='Spending Occurs Every Month'),
        Patch(facecolor='#5e81ac' , label='Sporadic Spending')
    ]
    
    ax.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.grid(alpha=0.25)
    plt.show()

def plot_monthly_budget_history(db: Session, period: str = 'full_history', include_wedding: bool = False) -> None:
    df = calculate_monthly_budget(db, period, include_wedding)
    fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    x = range(len(df))
    width = 0.35
    
    # ===== TOP PLOT: Earning vs Spending =====
    bars1 = ax1.bar([i - width/2 for i in x], df['monthly_salary'], width, 
                    label='Earning', color='#8fbcbb', alpha=0.8)
    bars2 = ax1.bar([i + width/2 for i in x], df['monthly_spending'], width, 
                    label='Spending', color='#81a1c1', alpha=0.8)
    
    # Net surplus/deficit line
    ax2 = ax1.twinx()
    df['net'] = df['monthly_spending'] - df['monthly_salary']
    line1 = ax2.plot(x, df['net'], color='#1976D2', marker='o', linewidth=2, 
                     markersize=6, label='Net (Surplus/Deficit)')
    ax2.axhline(0, color='black', linewidth=1, linestyle='--', alpha=0.5)
    ax2.set_ylabel('Net Amount ($)')
    
    ax1.set_ylabel('Amount ($)')
    ax1.set_title('Monthly Earning vs Spending')
    ax1.spines['top'].set_visible(False)
    
    # Combine legends for top plot
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.grid(alpha=0.25)
    
    # ===== BOTTOM PLOT: Savings Trajectory =====
    bars3 = ax3.bar(x, df['monthly_savings'], color='#88c0d0', alpha=0.7, 
                    label='Monthly Savings')
    
    # Highlight negative months
    negative_mask = df['monthly_savings'] < 0
    if negative_mask.any():
        ax3.bar([i for i, neg in enumerate(negative_mask) if neg], 
                df.loc[negative_mask, 'monthly_savings'], 
                color='#5e81ac', alpha=0.7)
    
    # Cumulative savings line
    ax4 = ax3.twinx()
    line2 = ax4.plot(x, df['cumulative_savings'], color='#434c5e', marker='o', 
                     linewidth=2.5, markersize=7, label='Cumulative Savings')
    ax4.set_ylabel('Cumulative Savings ($)', color='#434c5e')
    ax4.tick_params(axis='y', labelcolor='#434c5e')
    ax4.axhline(0, color='black', linewidth=0.8, linestyle='--', alpha=0.4)
    
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Monthly Savings ($)')
    ax3.set_title('Savings Trajectory')
    ax3.set_xticks(x)
    ax3.set_xticklabels(df['year_month'], rotation=45, ha='right')
    ax3.axhline(0, color='black', linewidth=0.8, linestyle='--', alpha=0.4)
    ax3.spines['top'].set_visible(False)
    
    # Combine legends for bottom plot
    lines3, labels3 = ax3.get_legend_handles_labels()
    lines4, labels4 = ax4.get_legend_handles_labels()
    ax3.legend(lines3 + lines4, labels3 + labels4, loc='best')
    
    plt.tight_layout()
    plt.grid(alpha=0.25)
    plt.show()


def plot_average_monthly_budget(db: Session, period: str = "full_history", include_wedding: bool = True) -> None:
    df = calculate_average_monthly_budget(db, period, include_wedding)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Separate income, spending, and cash flow
    income_df = df[df['category'] == 'INCOME'].copy()
    spending_df = df[df['category'] == 'SPENDING'].copy()
    cashflow_df = df[df['category'] == 'CASH_FLOW'].copy()
    
    # Sort spending by amount (most negative first)
    spending_df = spending_df.sort_values('amount')
    
    # Build waterfall sequence: income → spending → cash flow
    waterfall_df = pd.concat([income_df, spending_df, cashflow_df], ignore_index=True)
    
    # Calculate cumulative position for each bar
    cumulative = 0
    starts = []
    for amt in waterfall_df['amount']:
        starts.append(cumulative)
        cumulative += amt
    
    # Plot bars
    x = range(len(waterfall_df))
    colors = ['#2E7D32' if cat == 'INCOME' else '#D32F2F' if cat == 'SPENDING' else '#1976D2' 
              for cat in waterfall_df['category']]
    
    # Draw bars from start position
    for i, (start, amt, color) in enumerate(zip(starts, waterfall_df['amount'], colors)):
        ax.bar(i, amt, bottom=start, color=color, alpha=0.8, edgecolor='black', linewidth=0.5)
    
    # Connect bars with lines to show flow
    for i in range(len(waterfall_df) - 1):
        ax.plot([i + 0.4, i + 0.6], [starts[i] + waterfall_df.iloc[i]['amount']] * 2, 
                'k--', linewidth=0.8, alpha=0.4)
    
    # Add value labels on bars
    for i, (start, amt) in enumerate(zip(starts, waterfall_df['amount'])):
        label_y = start + amt/2
        ax.text(i, label_y, f'${abs(amt):.0f}', ha='center', va='center', 
                fontsize=8, fontweight='bold', color='white')
    
    # Format axes
    ax.set_xticks(x)
    ax.set_xticklabels(waterfall_df['description'], rotation=45, ha='right')
    ax.set_ylabel('Amount ($)')
    ax.set_title('Average Monthly Cash Flow Breakdown')
    ax.axhline(0, color='black', linewidth=1, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Legend
    legend_elements = [Patch(facecolor='#2E7D32', label='Income'),
                       Patch(facecolor='#D32F2F', label='Spending'),
                       Patch(facecolor='#1976D2', label='Cash Flow')]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.show()