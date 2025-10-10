import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
import logging
import numpy as np
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_BASE = "http://localhost:8000/api"

# Set matplotlib and seaborn styling
plt.style.use('default')
sns.set_palette("husl")

# Streamlit page configuration
st.set_page_config(
    page_title="Personal Finance Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
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
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_api_data(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
    """
    Fetch data from FastAPI endpoint with caching and error handling.
    
    Args:
        endpoint: API endpoint (e.g., '/spending/by-category')
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

def create_spending_trend_chart(data: list) -> plt.Figure:
    """
    Create a line chart showing monthly spending trends using matplotlib.
    
    Args:
        data: List of spending data points from API
    
    Returns:
        Matplotlib figure object
    """
    if not data:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', 
                transform=ax.transAxes, fontsize=14)
        ax.set_title("Monthly Spending Trend")
        return fig
    
    df = pd.DataFrame(data)
    
    # Create date column for proper time series plotting
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2))
    df = df.sort_values('date')
    
    # Create the figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Main spending line
    ax.plot(df['date'], df['spending'], 
            marker='o', linewidth=2.5, markersize=6,
            color='#e74c3c', label='Monthly Spending')
    
    # Add trend line if we have enough data
    if len(df) > 2:
        # Calculate trend using numpy polyfit
        x_numeric = np.arange(len(df))
        z = np.polyfit(x_numeric, df['spending'], 1)
        trend_line = np.poly1d(z)(x_numeric)
        
        ax.plot(df['date'], trend_line, 
                linestyle='--', linewidth=2, alpha=0.7,
                color='#3498db', label='Trend')
    
    # Formatting
    ax.set_title('Monthly Spending Trend', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Spending ($)', fontsize=12)
    
    # Format y-axis as currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.xticks(rotation=45)
    
    # Add grid and legend
    ax.grid(True, alpha=0.3)
    ax.legend(frameon=True, fancybox=True, shadow=True)
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    return fig

def create_category_chart(data: list, chart_type: str = "bar") -> plt.Figure:
    """
    Create a bar or pie chart showing spending by category using matplotlib.
    
    Args:
        data: List of category spending data from API
        chart_type: 'bar' or 'pie'
    
    Returns:
        Matplotlib figure object
    """
    if not data:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', 
                transform=ax.transAxes, fontsize=14)
        ax.set_title("Spending by Category")
        return fig
    
    df = pd.DataFrame(data)
    
    # Sort by spending and take top 10 for readability
    df = df.sort_values('total_spending', ascending=False).head(10)
    
    if chart_type == "pie":
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create pie chart
        wedges, texts, autotexts = ax.pie(
            df['total_spending'], 
            labels=[cat.replace('_', ' ').title() for cat in df['category']],
            autopct='%1.1f%%',
            startangle=90,
            colors=sns.color_palette("husl", len(df))
        )
        
        # Improve text formatting
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)
        
        for text in texts:
            text.set_fontsize(10)
        
        ax.set_title('Spending by Category', fontsize=16, fontweight='bold', pad=20)
        
    else:  # horizontal bar chart
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Clean up category names for display
        categories = [cat.replace('_', ' ').title() for cat in df['category']]
        
        # Create horizontal bar chart
        bars = ax.barh(categories, df['total_spending'], 
                      color=sns.color_palette("viridis", len(df)))
        
        # Add value labels on bars
        for i, (bar, value) in enumerate(zip(bars, df['total_spending'])):
            ax.text(bar.get_width() + value * 0.01, bar.get_y() + bar.get_height()/2,
                   f'${value:,.0f}', va='center', ha='left', fontweight='bold')
        
        # Formatting
        ax.set_title('Spending by Category', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Total Spending ($)', fontsize=12)
        ax.set_ylabel('Category', fontsize=12)
        
        # Format x-axis as currency
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add grid for easier reading
        ax.grid(axis='x', alpha=0.3)
        
        # Invert y-axis so highest spending is at top
        ax.invert_yaxis()
    
    plt.tight_layout()
    return fig

def display_summary_metrics(spending_data: list, category_data: list):
    """Display key metrics in a clean row format"""
    if not spending_data and not category_data:
        st.warning("No data available for metrics")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate summary metrics
    latest_spending = spending_data[-1]["spending"] if spending_data else 0
    avg_spending = sum(item["spending"] for item in spending_data) / len(spending_data) if spending_data else 0
    total_categories = len(category_data) if category_data else 0
    top_category = category_data[0]["category"].replace('_', ' ').title() if category_data else "N/A"
    
    with col1:
        st.metric(
            label="Latest Month",
            value=f"${latest_spending:,.0f}",
            delta=f"${latest_spending - avg_spending:,.0f}" if len(spending_data) > 1 else None
        )
    
    with col2:
        st.metric(
            label="Monthly Average",
            value=f"${avg_spending:,.0f}"
        )
    
    with col3:
        st.metric(
            label="Categories",
            value=total_categories
        )
    
    with col4:
        st.metric(
            label="Top Category",
            value=top_category
        )

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<div class="main-header">', unsafe_allow_html=True)
    st.title("💰 Personal Finance Dashboard")
    st.markdown("Track your spending patterns and financial trends")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    
    # Time period selection
    months = st.sidebar.selectbox(
        "Time Period",
        options=[6, 12, 18, 24],
        index=1,  # Default to 12 months
        format_func=lambda x: f"Last {x} months"
    )
    
    # Category level selection
    category_level = st.sidebar.selectbox(
        "Category Detail Level",
        options=["meta_category", "category"],
        index=0,  # Default to meta_category
        format_func=lambda x: "High-Level Categories" if x == "meta_category" else "Detailed Categories"
    )
    
    # Chart type for categories
    category_chart_type = st.sidebar.radio(
        "Category Chart Type",
        options=["bar", "pie"],
        index=0
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
        # Load spending trend data
        spending_response = fetch_api_data("/spending/monthly", {"months": months})
        spending_data = spending_response.get("data", []) if spending_response else []
        
        # Load category data
        category_response = fetch_api_data("/spending/by-category", {
            "period": "last_12_months",
            "level": category_level
        })
        category_data = category_response.get("data", []) if category_response else []
    
    # Display summary metrics
    if spending_data or category_data:
        display_summary_metrics(spending_data, category_data)
        st.markdown("---")
    
    # Main dashboard layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📈 Spending Trends")
        if spending_data:
            spending_chart = create_spending_trend_chart(spending_data)
            st.pyplot(spending_chart, clear_figure=True)
            
            # Show data table in expander
            with st.expander("📊 View Raw Spending Data"):
                df_display = pd.DataFrame(spending_data)
                if not df_display.empty:
                    # Format for display
                    df_display['spending_formatted'] = df_display['spending'].apply(lambda x: f"${x:,.2f}")
                    df_display['date'] = df_display['year'].astype(str) + '-' + df_display['month'].astype(str).str.zfill(2)
                    display_df = df_display[['date', 'spending_formatted']].rename(columns={
                        'date': 'Month',
                        'spending_formatted': 'Spending'
                    })
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No spending trend data available. Check your API connection.")
    
    with col2:
        st.subheader("🎯 Category Breakdown")
        if category_data:
            category_chart = create_category_chart(category_data, category_chart_type)
            st.pyplot(category_chart, clear_figure=True)
            
            # Show data table in expander
            with st.expander("📊 View Raw Category Data"):
                df_display = pd.DataFrame(category_data)
                if not df_display.empty:
                    # Format for display
                    df_display['total_spending_formatted'] = df_display['total_spending'].apply(lambda x: f"${x:,.2f}")
                    df_display['category_formatted'] = df_display['category'].apply(lambda x: x.replace('_', ' ').title())
                    display_df = df_display[['category_formatted', 'total_spending_formatted']].rename(columns={
                        'category_formatted': 'Category',
                        'total_spending_formatted': 'Total Spending'
                    }).sort_values('total_spending_formatted', key=lambda x: x.str.replace('$', '').str.replace(',', '').astype(float), ascending=False)
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No category data available. Check your API connection.")
    
    # Footer with metadata and data source info
    if spending_response or category_response:
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if spending_response and "metadata" in spending_response:
                metadata = spending_response["metadata"]
                st.caption(f"📈 Spending data: {metadata.get('count', 0)} data points")
        
        with col2:
            if category_response and "metadata" in category_response:
                metadata = category_response["metadata"]
                st.caption(f"🎯 Categories: {metadata.get('count', 0)} categories")
                st.caption(f"💰 Total tracked: ${metadata.get('total', 0):,.0f}")
        
        with col3:
            st.caption(f"🔄 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()