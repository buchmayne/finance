import pandas as pd
import numpy as np
from sqlalchemy.orm.session import Session

MORTGAGE_CONTRIBUTION = 850

def determine_last_X_months_from_dataset(
    df: pd.DataFrame, number_of_months: int
) -> np.array:
    """
    Given transaction data determine the most recent X year-months from the dataset
    """
    return (
        df["year_month"]
        .drop_duplicates()
        .sort_values(ascending=False)
        .reset_index(drop=True)
        .iloc[:number_of_months]
        .values
    )


def include_wedding_spending(
    df: pd.DataFrame, include_wedding: bool = True
) -> pd.DataFrame:
    """
    Wedding spending is a large outlier in terms of spending patterns and is not a repeatable expense.
    For the sake of financial planning it is useful to be able to toggle whether or not wedding expenses
    are included in the analysis dataset.
    """
    if not include_wedding:
        return df.loc[df["meta_category"] != "WEDDING"]
    else:
        return df


def subset_data_by_period(
    df: pd.DataFrame, period: str = "full_history"
) -> pd.DataFrame:
    """
    Subset dataset based on inputed time period.
    Options include year to date, last 1/3/6/12 months of data, or the full dataset.

    df: pd.DataFrame  = any marts table data
    period: str = 'ytd', 'last_1_months', 'last_3_months', 'last_6_months', 'last_12_months', 'full_history'
    """
    if period == "ytd":
        data = df.loc[df["year"] == df["year"].max()]
    elif period == "last_1_months":
        data = df.loc[
            df["year_month"].isin(
                determine_last_X_months_from_dataset(df, number_of_months=1)
            )
        ]
    elif period == "last_3_months":
        data = df.loc[
            df["year_month"].isin(
                determine_last_X_months_from_dataset(df, number_of_months=3)
            )
        ]
    elif period == "last_6_months":
        data = df.loc[
            df["year_month"].isin(
                determine_last_X_months_from_dataset(df, number_of_months=6)
            )
        ]
    elif period == "last_12_months":
        data = df.loc[
            df["year_month"].isin(
                determine_last_X_months_from_dataset(df, number_of_months=12)
            )
        ]
    elif period == "full_history":
        data = df
    else:
        raise ValueError(
            "Invalid period parameter. period can be 'ytd', 'last_1_months', 'last_3_months', 'last_6_months', 'last_12_months', 'full_history'"
        )
    return data


def calculate_average_monthly_spending_by_meta_category(
    db: Session, period: str = "full_history", include_wedding: bool = True
) -> pd.DataFrame:
    """
    View of average monthly spending patterns.
    """
    data = (
        pd.read_sql("""SELECT * FROM marts_spending""", db.connection())
        .pipe(subset_data_by_period, period)
        .pipe(include_wedding_spending, include_wedding)
    )

    monthly_data = data.groupby(["year_month", "meta_category"], as_index=False).agg(
        total_spend=("amount", "sum"),
        monthly_transactions=("amount", "count"),
    )

    number_of_months = monthly_data["year_month"].unique().shape[0]

    return (
        monthly_data.groupby(["meta_category"], as_index=False)
        .agg(
            total_spend=("total_spend", "sum"),
            avg_monthly_transactions=("monthly_transactions", "mean"),
            months_with_transactions=("total_spend", "count"),
        )
        .assign(avg_monthly_spend=lambda df_: df_["total_spend"] / number_of_months)
        .assign(
            pct_of_avg_monthly_spend=lambda df_: round(
                df_["avg_monthly_spend"] / df_["avg_monthly_spend"].sum(), 4
            )
            * 100,
            number_of_months_in_sample=number_of_months,
        )
        .assign(
            spending_occurs_every_month=lambda df_: df_["number_of_months_in_sample"]
            == df_["months_with_transactions"]
        )
        .sort_values("avg_monthly_spend", ascending=True)[
            [
                "meta_category",
                "number_of_months_in_sample",
                "months_with_transactions",
                "spending_occurs_every_month",
                "avg_monthly_transactions",
                "total_spend",
                "avg_monthly_spend",
                "pct_of_avg_monthly_spend",
            ]
        ]
    )


def calculate_monthly_spending(
    db: Session, period: str = "full_history", include_wedding: bool = True
) -> pd.DataFrame:
    return (
        pd.read_sql("""SELECT * FROM marts_spending""", db.connection())
        .pipe(subset_data_by_period, period)
        .pipe(include_wedding_spending, include_wedding)
        .groupby(["year_month"], as_index=False)
        .agg(monthly_spending=("amount", "sum"))
    )


def calculate_monthly_saving(db: Session, period: str = "full_history") -> pd.DataFrame:
    """
    Calculate savings per month over inputed time period
    """
    return (
        pd.read_sql("""SELECT * FROM marts_savings""", db.connection())
        .pipe(subset_data_by_period, period)
        .assign(year_month=lambda df_: pd.to_datetime(df_["year_month"]))
        .groupby(["year_month"])
        .agg(monthly_savings=("amount", "sum"))
        .pipe(
            lambda df_: df_.reindex(
                pd.date_range(start=df_.index.min(), end=df_.index.max(), freq="MS"),
                fill_value=0,
            )
        )
        .reset_index(names="year_month")
        .assign(
            year_month=lambda df_: df_["year_month"].astype(str).str[:7]
        )  # convert to format used elsewhere for join
    )


def calculate_average_monthly_saving(
    db: Session, period: str = "full_history"
) -> float:
    """
    For the given time period calculate average monthly savings
    """
    return float(calculate_monthly_saving(db, period)["monthly_savings"].mean())


def calculate_monthly_salary(db: Session, period: str = "full_history") -> pd.DataFrame:
    """
    Calculate my monthly salary income for the given time period.
    """
    return (
        pd.read_sql("""SELECT * FROM marts_income""", db.connection())
        .pipe(subset_data_by_period, period)
        .loc[lambda df_: df_["category"] == "SALARY"]
        .groupby(["year_month"], as_index=False)
        .agg(monthly_salary=("amount", "sum"))
    )


def calculate_average_monthly_salary(
    db: Session, period: str = "full_history"
) -> float:
    """
    For the given time period calculate average monthly salary
    """
    return float(calculate_monthly_salary(db, period)["monthly_salary"].mean())


def calculate_average_monthly_budget(
    db: Session, period: str = "full_history", include_wedding: bool = True
) -> pd.DataFrame:
    """
    Calculate the average spending and income per month over a given period to
    get a monthly cash flow estimate for budgeting purposes.
    """
    spending = (
        calculate_average_monthly_spending_by_meta_category(
            db, period, include_wedding
        )[["meta_category", "avg_monthly_spend"]]
        .rename(columns={"meta_category": "description", "avg_monthly_spend": "amount"})
        .assign(amount=lambda df_: df_["amount"] * -1, category="SPENDING")
    )

    income = pd.DataFrame(
        {
            "description": ["SALARY", "MORTGAGE_CONTRIBUTION"],
            "amount": [
                calculate_average_monthly_salary(db, period),
                MORTGAGE_CONTRIBUTION,
            ],
            "category": ["INCOME", "INCOME"],
        },
        index=[0, 1],
    )

    return pd.concat([spending, income], axis=0).pipe(
        lambda df_: pd.concat(
            [
                df_,
                pd.DataFrame(
                    {
                        "description": "CASH_FLOW",
                        "amount": df_["amount"].sum(),
                        "category": "CASH_FLOW",
                    },
                    index=[0],
                ),
            ],
            axis=0,
        )
    )


def calculate_monthly_budget_history(
    db: Session, period: str = "full_history", include_wedding: bool = True
) -> pd.DataFrame:
    """
    For each month in the time period calculate total spending, income, and savings.
    """
    return (
        calculate_monthly_spending(db, period, include_wedding)
        .merge(calculate_monthly_salary(db, period), how="left")
        .merge(calculate_monthly_saving(db, period), how="left")
        .assign(cumulative_savings=lambda df_: df_["monthly_savings"].cumsum())
    )
