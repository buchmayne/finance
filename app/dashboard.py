import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging
from typing import Optional, Dict, Any
from visualizations import plot_monthly_spending_by_category

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_BASE = "http://localhost:8000/api"

# Set matplotlib and seaborn styling
plt.style.use("default")
sns.set_palette("husl")

# Streamlit page configuration
st.set_page_config(
    page_title="Personal Finance Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .main-header {
        padding: 1rem 0;
        border-bottom: 2px solid #f0f2f6;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e1e5e9;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .stSelectbox > div > div {
        background-color: white;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_api_data(
    endpoint: str, params: Optional[Dict[str, Any]] = None
) -> Optional[Dict]:
    """
    Fetch data from FastAPI endpoint with caching and error handling.

    Args:
        endpoint: API endpoint (e.g., '/metrics/spending-by-category')
        params: Query parameters as dictionary

    Returns:
        JSON response as dictionary, or None if request failed
    """
    try:
        url = f"{API_BASE}{endpoint}"
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {endpoint}: {e}")
        st.error(f"Failed to fetch data from {endpoint}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {endpoint}: {e}")
        st.error(f"Unexpected error: {str(e)}")
        return None


# plot_monthly_spending_by_category
def create_spending_by_category_visualization(data: list) -> plt.Figure:
    """
    TBD
    """
    if not data:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(
            0.5,
            0.5,
            "No data available",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=14,
        )
        ax.set_title("Spending by Category")
        return fig

    df = pd.DataFrame(data, index=list(range(len(data))))
    fig = plot_monthly_spending_by_category(df)

    return fig


def main():
    """Main Streamlit application"""

    # Header
    st.markdown('<div class="main-header">', unsafe_allow_html=True)
    st.title("💰 Personal Finance Dashboard")
    st.markdown("Track your spending patterns and financial trends")
    st.markdown("</div>", unsafe_allow_html=True)

    # Sidebar controls
    st.sidebar.header("Dashboard Controls")

    # Time period selection
    time_period = st.sidebar.selectbox(
        "Time Period",
        options=[
            "ytd",
            "last_1_months",
            "last_3_months",
            "last_6_months",
            "last_12_months",
            "full_history",
        ],
        index=4,  # Default to 12 months
    )

    # Category level selection
    include_wedding = st.sidebar.selectbox(
        "Include Wedding Expenses in Analysis",
        options=[True, False],
        index=1,  # Default to omitting Wedding Expenses
    )

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.cache_data.clear()
        st.experimental_rerun()

    # Display API connection status
    st.sidebar.markdown("---")
    st.sidebar.markdown("**API Status**")
    try:
        health_check = requests.get(f"{API_BASE}/health", timeout=5)
        if health_check.status_code == 200:
            st.sidebar.success("✅ Connected to API")
        else:
            st.sidebar.error("❌ API Error")
    except:
        st.sidebar.error("❌ Cannot connect to API")

    # Data loading with progress indicators
    with st.spinner("Loading financial data..."):
        # Load category data
        category_response = fetch_api_data(
            "/metrics/spending-by-category",
            {"period": time_period, "include_wedding": include_wedding},
        )

        category_data = category_response if category_response else []

    # Main dashboard layout
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("🎯 Category Breakdown")
        if category_data:
            category_chart = create_spending_by_category_visualization(category_data)
            st.pyplot(category_chart, clear_figure=True)

            # Show data table in expander
            with st.expander("📊 View Raw Category Data"):
                df_display = pd.DataFrame(
                    category_data, index=list(range(len(category_data)))
                )
                if not df_display.empty:
                    # Format for display
                    df_display["avg_monthly_spend_formatted"] = df_display[
                        "avg_monthly_spend"
                    ].apply(lambda x: f"${x:,.2f}")
                    df_display["category_formatted"] = df_display[
                        "meta_category"
                    ].apply(lambda x: x.replace("_", " ").title())
                    display_df = (
                        df_display[
                            ["category_formatted", "avg_monthly_spend_formatted"]
                        ]
                        .rename(
                            columns={
                                "category_formatted": "Category",
                                "avg_monthly_spend_formatted": "Average Spending",
                            }
                        )
                        .sort_values(
                            "Average Spending",
                            key=lambda x: x.str.replace("$", "")
                            .str.replace(",", "")
                            .astype(float),
                            ascending=False,
                        )
                    )
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No category data available. Check your API connection.")

    # Footer with metadata and data source info
    if category_response:
        st.markdown("---")
        col1, col2, col3 = st.columns(3)

        with col1:
            if category_response and "metadata" in category_response:
                metadata = category_response["metadata"]
                st.caption(f"🎯 Categories: {metadata.get('count', 0)} categories")
                st.caption(f"💰 Total tracked: ${metadata.get('total', 0):,.0f}")

        with col2:
            st.caption(
                f"🔄 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )


if __name__ == "__main__":
    main()
