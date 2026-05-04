import streamlit as st
import pandas as pd
import plotly.express as px

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TMDB Executive Dashboard", layout="wide")

# --- DATABASE CONNECTION ---
# Assumes credentials are in .streamlit/secrets.toml
conn = st.connection('mysql', type='sql')

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to:", ["Market Share Analysis", "ROI Efficiency"])

# --- PAGE 1: MARKET SHARE ANALYSIS ---
if page == "Market Share Analysis":
    st.title("🌏 Global Market Share Analysis")
    
    # 1. Sidebar Filters for this specific page
    countries_df = conn.query("SELECT country_name FROM country ORDER BY country_name;")
    selected_country = st.sidebar.selectbox("Select Country", options=countries_df['country_name'], index=0)
    top_n_market = st.sidebar.slider("Number of Companies", 3, 20, 10)

    # 2. Query execution
    market_query = f"""
    SELECT 
        c.company_name,
        SUM(m.revenue) as company_revenue
    FROM company c
    JOIN movie_company mc ON c.company_id = mc.company_id
    JOIN movie m ON mc.movie_id = m.movie_id
    JOIN movie_country mcn ON m.movie_id = mcn.movie_id
    JOIN country cn ON mcn.country_id = cn.country_id
    WHERE cn.country_name = '{selected_country}' AND m.revenue > 0 
    GROUP BY c.company_id, c.company_name
    ORDER BY company_revenue DESC
    LIMIT {top_n_market};
    """
    df_market = conn.query(market_query)

    if not df_market.empty:
        fig_pie = px.pie(df_market, values='company_revenue', names='company_name', 
                         title=f"Top {top_n_market} Companies in {selected_country}", hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)
        st.dataframe(df_market, use_container_width=True, hide_index=True)
    else:
        st.warning(f"No data for {selected_country}")

# --- PAGE 2: ROI EFFICIENCY ---
elif page == "ROI Efficiency":
    st.title("💰 Production Efficiency (ROI)")
    
    # 1. Sidebar Filters for ROI
   
    with st.sidebar:
        st.header("Efficiency Filters")
        min_movies = st.number_input("Minimum Movies Produced", min_value=1, value=6)
        top_n_roi = st.slider("Show Top N ROI Leaders", 5, 20, 10)
        
        # Year Range Slider
        year_range = st.slider(
            "Select Year Range",
            min_value=1950, 
            max_value=2026, 
            value=(2000, 2026) # Default range
        )

    # 2. Query execution
    roi_query = f"""
    WITH CompanyROI AS (
        SELECT 
            c.company_name,
            COUNT(m.movie_id) AS movie_count,
            SUM(m.budget) AS total_investment,
            SUM(m.revenue) AS total_return,
            (SUM(m.revenue) - SUM(m.budget)) / NULLIF(SUM(m.budget), 0) AS roi_multiplier
        FROM company c
        JOIN movie_company mc ON c.company_id = mc.company_id
        JOIN movie m ON mc.movie_id = m.movie_id
        WHERE m.budget > 1000000 
          AND m.revenue > 0
          AND YEAR(m.release_date) BETWEEN {year_range[0]} AND {year_range[1]}
        GROUP BY c.company_id, c.company_name
    )
    SELECT * FROM CompanyROI 
    WHERE movie_count >= {min_movies}
    ORDER BY roi_multiplier DESC 
    LIMIT {top_n_roi};
    """
    df_roi = conn.query(roi_query)

    if not df_roi.empty:
        fig_bar = px.bar(
            df_roi, 
            x='roi_multiplier', 
            y='company_name', 
            orientation='h',
            title=f"ROI Leaders ({year_range[0]} - {year_range[1]})",
            color='roi_multiplier',
            color_continuous_scale='Viridis',
            hover_data={'total_investment': ':$,.0f', 'total_return': ':$,.0f'}
        )
        
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
        
        # Formatted Data Table for SJSU Project Clarity
        st.dataframe(
            df_roi.style.format({
                'roi_multiplier': '{:.2f}x', 
                'total_investment': '${:,.0f}', 
                'total_return': '${:,.0f}'
            }), 
            use_container_width=True, 
            hide_index=True
        )
    else:
        st.warning(f"No companies met the criteria for the years {year_range[0]}-{year_range[1]}.")