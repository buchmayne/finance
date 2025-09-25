import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import logging
from typing import Optional, Dict, Any


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "http://localhost:8000/api"

st.set_page_config(
    page_title="Personal Finance Dashboad",
    page_icon="$",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better look
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
        endpoint: API endpoint (e.g., '/transactions/monthly_cash_flow')
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

def create_monthly_cash_flow_chart(data: list) -> go.Figure:
    """
    Line chart showing monthly cash flow
    """
    if not data:
        return go.Figure().add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False
        )
    
    df = pd.DataFrame(data)

    # Create date column for better x-axis handling
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2))

    fig = go.Figure()

    # Add cash flow line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['cash_flow'],
        mode='lines+markers',
        name='Monthly Cash Flow',
        line=dict(color='#ff6b6b', width=3),
        marker=dict(size=8, symbol='circle'),
        hovertemplate='<b>%{x|%B %Y}</b><br>' +
                      'Cash Flow: $%{y:,.0f}<br>' +
                      '<extra></extra>'
    ))

    # Add trend line
    if len(df) > 1:
        z = np.polyfit(range(len(df)), df['cash_flow'], 1)
        trend_line = np.poly1d(z)(range(len(df)))
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=trend_line,
            mode='lines',
            name='Trend',
            line=dict(color='#4ecdc4', width=2, dash='dash'),
            hovertemplate='Trend: $%{y:,.0f}<extra></extra>'
        ))
    
    fig.update_layout(
        title="Monthly Cash Flow Trend",
        xaxis_title="Month",
        yaxis_title="Cash Flow ($)",
        hovermode='x unified',
        template='plotly_white',
        height=400,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    # Format y-axis as currency
    fig.update_yaxes(tickformat='$,.0f')
    
    return fig


def create_category_chart(data: list, chart_type: str = "bar") -> go.Figure:
    """
    Create a bar or pie chart showing spending by category.
    
    Args:
        data: List of category spending data from API
        chart_type: 'bar' or 'pie'
    
    Returns:
        Plotly figure object
    """
    if not data:
        return go.Figure().add_annotation(text="No data available", 
                                        xref="paper", yref="paper",
                                        x=0.5, y=0.5, showarrow=False)
    
    df = pd.DataFrame(data)
    
    # Sort by spending and take top 10 for readability
    df = df.sort_values('total_spending', ascending=False).head(10)
    
    if chart_type == "pie":
        fig = go.Figure(data=[go.Pie(
            labels=df['category'],
            values=df['total_spending'],
            textinfo='label+percent',
            textposition='auto',
            hovertemplate='<b>%{label}</b><br>' +
                         'Amount: $%{value:,.0f}<br>' +
                         'Percentage: %{percent}<br>' +
                         '<extra></extra>'
        )])
        
        fig.update_layout(
            title="Spending by Category",
            height=500,
            margin=dict(l=0, r=0, t=40, b=0)
        )
        
    else:  # bar chart
        fig = go.Figure(data=[go.Bar(
            x=df['total_spending'],
            y=df['category'],
            orientation='h',
            marker_color='#4ecdc4',
            hovertemplate='<b>%{y}</b><br>' +
                         'Amount: $%{x:,.0f}<br>' +
                         '<extra></extra>'
        )])
        
        fig.update_layout(
            title="Spending by Category",
            xaxis_title="Total Spending ($)",
            yaxis_title="Category",
            height=400,
            margin=dict(l=0, r=0, t=40, b=0),
            template='plotly_white'
        )
        
        # Format x-axis as currency
        fig.update_xaxes(tickformat='$,.0f')
        
        # Sort y-axis by spending amount
        fig.update_yaxes(categoryorder='total ascending')
    
    return fig

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<div class="main-header">', unsafe_allow_html=True)
    st.title("💰 Personal Finance Dashboard")
    st.markdown("Track spending patterns and financial trends")
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
    
    # Data loading with progress indicators
    with st.spinner("Loading financial data..."):
        # Load spending trend data
        spending_response = fetch_api_data("/transactions/monthly_cash_flow", {"months": months})
        spending_data = spending_response.get("data", []) if spending_response else []
        
        # Load category data
        category_response = fetch_api_data("/transactions/by-category", {
            "period": "last_12_months",
            "level": category_level
        })
        category_data = category_response.get("data", []) if category_response else []
    
    # Display metrics row
    if spending_data and category_data:
        col1, col2, col3, col4 = st.columns(4)
        
        # Calculate summary metrics
        latest_spending = spending_data[-1]["cash_flow"] if spending_data else 0
        avg_spending = sum(item["cash_flow"] for item in spending_data) / len(spending_data) if spending_data else 0
        total_categories = len(category_data)
        top_category = category_data[0]["category"] if category_data else "N/A"
        
        with col1:
            st.metric(
                label="Latest Month Spending",
                value=f"${latest_spending:,.0f}",
                delta=f"${latest_spending - avg_spending:,.0f}" if len(spending_data) > 1 else None
            )
        
        with col2:
            st.metric(
                label="Average Monthly Spending",
                value=f"${avg_spending:,.0f}"
            )
        
        with col3:
            st.metric(
                label="Active Categories",
                value=total_categories
            )
        
        with col4:
            st.metric(
                label="Top Category",
                value=top_category.replace("_", " ").title()
            )
    
    # Main dashboard layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Spending Trends")
        if spending_data:
            spending_chart = create_monthly_cash_flow_chart(spending_data)
            st.plotly_chart(spending_chart, use_container_width=True)
            
            # Show data table in expander
            with st.expander("View Raw Data"):
                df_display = pd.DataFrame(spending_data)
                if not df_display.empty:
                    df_display['cash_flow'] = df_display['cash_flow'].apply(lambda x: f"${x:,.2f}")
                st.dataframe(df_display, use_container_width=True)
        else:
            st.warning("No spending trend data available")
    
    with col2:
        st.subheader("🎯 Category Breakdown")
        if category_data:
            category_chart = create_category_chart(category_data, category_chart_type)
            st.plotly_chart(category_chart, use_container_width=True)
            
            # Show data table in expander
            with st.expander("View Raw Data"):
                df_display = pd.DataFrame(category_data)
                if not df_display.empty:
                    df_display['total_spending'] = df_display['total_spending'].apply(lambda x: f"${x:,.2f}")
                st.dataframe(df_display, use_container_width=True)
        else:
            st.warning("No category data available")
    
    # Footer with metadata
    if spending_response or category_response:
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if spending_response and "metadata" in spending_response:
                metadata = spending_response["metadata"]
                st.caption(f"Spending data: {metadata.get('count', 0)} months")
        
        with col2:
            if category_response and "metadata" in category_response:
                metadata = category_response["metadata"]
                st.caption(f"Category data: {metadata.get('count', 0)} categories, "
                          f"${metadata.get('total', 0):,.0f} total")

if __name__ == "__main__":
    main()