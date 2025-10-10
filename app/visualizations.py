import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


# Visualization Functions
def plot_monthly_spending_by_category(df: pd.DataFrame) -> plt.figure:
    """
    df = calculate_average_monthly_spending_by_meta_category(
        db, period, include_wedding
    )
    """
    fig, ax = plt.subplots(figsize=(10, 8))

    # Binary color: one color for every-month spending, another for sporadic
    colors = [
        "#8fbcbb" if every_month else "#5e81ac"
        for every_month in df["spending_occurs_every_month"]
    ]

    bars = ax.barh(df["meta_category"], df["avg_monthly_spend"], color=colors)

    # Annotate with percentage
    for i, (spend, pct) in enumerate(
        zip(df["avg_monthly_spend"], df["pct_of_avg_monthly_spend"])
    ):
        ax.text(spend + 10, i, f"{pct:.1f}%", va="center", fontsize=9)

    ax.set_xlabel("Avg Monthly Spend ($)")
    ax.set_title("Monthly Budget Breakdown")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend_elements = [
        Patch(facecolor="#8fbcbb", label="Spending Occurs Every Month"),
        Patch(facecolor="#5e81ac", label="Sporadic Spending"),
    ]

    ax.legend(handles=legend_elements, loc="lower right")

    plt.tight_layout()
    plt.grid(alpha=0.25)

    return fig


def plot_monthly_budget_history(df: pd.DataFrame) -> None:
    """
    df = calculate_monthly_budget_history(db, period, include_wedding)
    """
    fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    x = range(len(df))
    width = 0.35

    # ===== TOP PLOT: Earning vs Spending =====
    bars1 = ax1.bar(
        [i - width / 2 for i in x],
        df["monthly_salary"],
        width,
        label="Earning",
        color="#8fbcbb",
        alpha=0.8,
    )
    bars2 = ax1.bar(
        [i + width / 2 for i in x],
        df["monthly_spending"],
        width,
        label="Spending",
        color="#81a1c1",
        alpha=0.8,
    )

    # Net surplus/deficit line
    ax2 = ax1.twinx()
    df["net"] = df["monthly_spending"] - df["monthly_salary"]
    line1 = ax2.plot(
        x,
        df["net"],
        color="#1976D2",
        marker="o",
        linewidth=2,
        markersize=6,
        label="Net (Surplus/Deficit)",
    )
    ax2.axhline(0, color="black", linewidth=1, linestyle="--", alpha=0.5)
    ax2.set_ylabel("Net Amount ($)")

    ax1.set_ylabel("Amount ($)")
    ax1.set_title("Monthly Earning vs Spending")
    ax1.spines["top"].set_visible(False)

    # Combine legends for top plot
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.grid(alpha=0.25)

    # ===== BOTTOM PLOT: Savings Trajectory =====
    bars3 = ax3.bar(
        x, df["monthly_savings"], color="#88c0d0", alpha=0.7, label="Monthly Savings"
    )

    # Highlight negative months
    negative_mask = df["monthly_savings"] < 0
    if negative_mask.any():
        ax3.bar(
            [i for i, neg in enumerate(negative_mask) if neg],
            df.loc[negative_mask, "monthly_savings"],
            color="#5e81ac",
            alpha=0.7,
        )

    # Cumulative savings line
    ax4 = ax3.twinx()
    line2 = ax4.plot(
        x,
        df["cumulative_savings"],
        color="#434c5e",
        marker="o",
        linewidth=2.5,
        markersize=7,
        label="Cumulative Savings",
    )
    ax4.set_ylabel("Cumulative Savings ($)", color="#434c5e")
    ax4.tick_params(axis="y", labelcolor="#434c5e")
    ax4.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.4)

    ax3.set_xlabel("Month")
    ax3.set_ylabel("Monthly Savings ($)")
    ax3.set_title("Savings Trajectory")
    ax3.set_xticks(x)
    ax3.set_xticklabels(df["year_month"], rotation=45, ha="right")
    ax3.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.4)
    ax3.spines["top"].set_visible(False)

    # Combine legends for bottom plot
    lines3, labels3 = ax3.get_legend_handles_labels()
    lines4, labels4 = ax4.get_legend_handles_labels()
    ax3.legend(lines3 + lines4, labels3 + labels4, loc="best")

    plt.tight_layout()
    plt.grid(alpha=0.25)
    plt.show()


def plot_average_monthly_budget(df: pd.DataFrame) -> None:
    """
    df = calculate_average_monthly_budget(db, period, include_wedding)
    """

    fig, ax = plt.subplots(figsize=(14, 8))

    # Separate income, spending, and cash flow
    income_df = df[df["category"] == "INCOME"].copy()
    spending_df = df[df["category"] == "SPENDING"].copy()
    cashflow_df = df[df["category"] == "CASH_FLOW"].copy()

    # Sort spending by amount (most negative first)
    spending_df = spending_df.sort_values("amount")

    # Build waterfall sequence: income → spending → cash flow
    waterfall_df = pd.concat([income_df, spending_df, cashflow_df], ignore_index=True)

    # Calculate cumulative position for each bar
    cumulative = 0
    starts = []
    for amt in waterfall_df["amount"]:
        starts.append(cumulative)
        cumulative += amt

    # Plot bars
    x = range(len(waterfall_df))
    colors = [
        "#2E7D32" if cat == "INCOME" else "#D32F2F" if cat == "SPENDING" else "#1976D2"
        for cat in waterfall_df["category"]
    ]

    # Draw bars from start position
    for i, (start, amt, color) in enumerate(
        zip(starts, waterfall_df["amount"], colors)
    ):
        ax.bar(
            i,
            amt,
            bottom=start,
            color=color,
            alpha=0.8,
            edgecolor="black",
            linewidth=0.5,
        )

    # Connect bars with lines to show flow
    for i in range(len(waterfall_df) - 1):
        ax.plot(
            [i + 0.4, i + 0.6],
            [starts[i] + waterfall_df.iloc[i]["amount"]] * 2,
            "k--",
            linewidth=0.8,
            alpha=0.4,
        )

    # Add value labels on bars
    for i, (start, amt) in enumerate(zip(starts, waterfall_df["amount"])):
        label_y = start + amt / 2
        ax.text(
            i,
            label_y,
            f"${abs(amt):.0f}",
            ha="center",
            va="center",
            fontsize=8,
            fontweight="bold",
            color="white",
        )

    # Format axes
    ax.set_xticks(x)
    ax.set_xticklabels(waterfall_df["description"], rotation=45, ha="right")
    ax.set_ylabel("Amount ($)")
    ax.set_title("Average Monthly Cash Flow Breakdown")
    ax.axhline(0, color="black", linewidth=1, alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend
    legend_elements = [
        Patch(facecolor="#2E7D32", label="Income"),
        Patch(facecolor="#D32F2F", label="Spending"),
        Patch(facecolor="#1976D2", label="Cash Flow"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    plt.tight_layout()
    plt.show()
