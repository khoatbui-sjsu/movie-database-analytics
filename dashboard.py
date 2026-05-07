import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


# Page config
st.set_page_config(
    page_title="TMDB Database Dashboard",
    page_icon=':chess:',
    layout="wide",
    initial_sidebar_state="expanded",
)


# CSS
def load_css(path: str):
    with open(path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css("styles/main.css")


# DB connection
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "101188",
    "database": "TMDBMovie",
}


@st.cache_resource
def get_engine():
    # if not HAS_SQL:
    #     st.error("SQLAlchemy not installed. Run: pip install sqlalchemy")
    #     st.stop()
    cfg = DB_CONFIG
    url = (
        f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}/{cfg['database']}"
    )
    try:
        engine = create_engine(url, poolclass=NullPool)
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()


def query(sql: str, params=None) -> pd.DataFrame:
    """Execute a %s-style parameterised query and return a DataFrame."""
    engine = get_engine()
    try:
        # Convert %s placeholders → :p0, :p1, ... for SQLAlchemy
        named_sql = sql
        named_params = {}
        if params:
            idx = 0
            while "%s" in named_sql:
                key = f"p{idx}"
                named_sql = named_sql.replace("%s", f":{key}", 1)
                named_params[key] = params[idx]
                idx += 1
        with engine.connect() as conn:
            return pd.read_sql(text(named_sql), conn, params=named_params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()



# Load filter options
@st.cache_data(ttl=600)
def load_genres():
    return query("SELECT genre_name FROM genre ORDER BY genre_name")["genre_name"].tolist()


@st.cache_data(ttl=600)
def load_languages():
    return query("SELECT language_code, language_name FROM language ORDER BY language_name")


@st.cache_data(ttl=600)
def load_year_range():
    df = query("SELECT MIN(YEAR(release_date)) AS mn, MAX(YEAR(release_date)) AS mx FROM movie WHERE release_date <= '2026-06-01' and release_date IS NOT NULL")
    return int(df["mn"][0] or 1900), int(df["mx"][0] or 2026)


# Sidebar — filters
with st.sidebar:
    st.markdown("# TMDB\nEXPLORER")
    st.markdown("---")

    st.markdown("### Filters")

    # Year range
    y_min, y_max = load_year_range()
    year_range = st.slider("Release Year", y_min, y_max, (2000, y_max))

    # Genre
    genres = load_genres()
    selected_genres = st.multiselect("Genres", genres, placeholder="All genres")

    # Language
    lang_df = load_languages()
    lang_options = ["All"] + lang_df["language_code"].tolist()
    lang_labels = {"All": "All Languages"}
    for _, r in lang_df.iterrows():
        lang_labels[r["language_code"]] = f"{r['language_code']} — {r['language_name'] or ''}"
    selected_lang = st.selectbox(
        "Original Language",
        lang_options,
        format_func=lambda c: lang_labels.get(c, c),
    )

    # Vote average
    min_rating = st.slider("Min Vote Average", 0.0, 10.0, 5.0, 0.1)

    # Min vote count (to exclude obscure entries)
    min_votes = st.number_input("Min Vote Count", 0, 100000, 100, step=100)

    # Adult content
    include_adult = st.toggle("Include Adult Content", value=False)

    # Sort
    sort_by = st.selectbox("Sort By", [
        "popularity DESC", "vote_average DESC", "revenue DESC",
        "release_date DESC", "release_date ASC", "runtime DESC"
    ])

    # Limit
    result_limit = st.select_slider("Max Results", [50, 100, 250, 500, 1000], value=250)

    st.markdown("---")
    run = st.button("  Apply Filters", width='stretch')


# Building dynamic query
def build_movie_query(year_range, selected_genres, selected_lang,
                       min_rating, min_votes, include_adult, sort_by, limit):
    wheres = [
        "YEAR(m.release_date) BETWEEN %s AND %s",
        "m.vote_average >= %s",
        "m.vote_count >= %s",
    ]
    params = [year_range[0], year_range[1], min_rating, min_votes]

    #if user enters selection
    if not include_adult:
        wheres.append("(m.adult = 0 OR m.adult IS NULL)")

    if selected_lang != "All":
        wheres.append("m.original_language_code = %s")
        params.append(selected_lang)

    genre_join = ""
    if selected_genres:
        placeholders = ", ".join(["%s"] * len(selected_genres))
        genre_join = """
            JOIN movie_genre mg ON mg.movie_id = m.movie_id
            JOIN genre g ON g.genre_id = mg.genre_id
        """
        wheres.append(f"g.genre_name IN ({placeholders})")
        params = params + selected_genres

    sql = f"""
        SELECT DISTINCT
            m.movie_id, m.title, m.release_date,
            m.vote_average, m.vote_count,
            m.revenue, m.budget, m.runtime,
            m.popularity, m.original_language_code,
            m.overview, m.tagline, m.status
        FROM movie m
        {genre_join}
        WHERE {" AND ".join(wheres)}
        ORDER BY {sort_by}
        LIMIT {int(limit)}
    """
    return sql, params


# Visuals display
st.markdown("# TMDB MOVIE EXPLORER DASHBOARD")
st.markdown("Explore 1M+ movies from The Movie Database")
st.markdown("---")

# Auto-run on first load
if "df" not in st.session_state or run:
    sql, params = build_movie_query(
        year_range, selected_genres, selected_lang,
        min_rating, min_votes, include_adult, sort_by, result_limit
    )
    with st.spinner("Querying database..."):
        st.session_state.df = query(sql, params)
df = st.session_state.df

if df.empty:
    st.warning("No results found. Try relaxing the filters.")
    st.stop()

# KPI row
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Movies Found", f"{len(df):,}")
k2.metric("Avg Rating", f"{df['vote_average'].mean():.2f}")
k3.metric("Avg Runtime", f"{df['runtime'].dropna().mean():.0f} min")
k4.metric("Total Revenue", f"${df['revenue'].sum()/1e9:.1f}B")
k5.metric("Avg Popularity", f"{df['popularity'].mean():.1f}")

st.markdown("---")

# Tabs
tab1, tab3, tab4, tab6, tab8 = st.tabs(["CHARTS", "GENRES", "LANGUAGES", "PRODUCTION", "TRENDS"])

#Tab 1: Charts
with tab1:
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Releases Per Year")
        year_counts = (
            df.dropna(subset=["release_date"])
            .assign(year=pd.to_datetime(df["release_date"]).dt.year)
            .groupby("year")
            .size()
            .reset_index(name="count")
        )
        fig = px.bar(
            year_counts, x="year", y="count",
            color="count", color_continuous_scale="Oranges",
        )
        fig.update_layout(
            plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
            font_color="#e8e0d0", coloraxis_showscale=False,
            xaxis=dict(gridcolor="#1a1a2a"), yaxis=dict(gridcolor="#1a1a2a"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, width='stretch')

    with c2:
        st.markdown("### Rating Distribution")
        fig2 = px.histogram(
            df, x="vote_average", nbins=20,
            color_discrete_sequence=["#f5c518"],
        )
        fig2.update_layout(
            plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
            font_color="#e8e0d0", showlegend=False,
            xaxis=dict(gridcolor="#1a1a2a"), yaxis=dict(gridcolor="#1a1a2a"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig2, width='stretch')

    c3, c4 = st.columns(2)

    with c3:
        st.markdown("### Top 15 by Revenue")
        top_rev = df[df["revenue"] > 0].nlargest(15, "revenue")
        fig3 = px.bar(
            top_rev, x="revenue", y="title", orientation="h",
            color="revenue", color_continuous_scale="YlOrRd",
        )
        fig3.update_layout(
            plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
            font_color="#e8e0d0", coloraxis_showscale=False,
            yaxis=dict(autorange="reversed", gridcolor="#1a1a2a"),
            xaxis=dict(gridcolor="#1a1a2a"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig3, width='stretch')

    with c4:
        st.markdown("### Popularity vs Rating")
        fig4 = px.scatter(
            df[df["vote_count"] > 50].sample(min(500, len(df))),
            x="vote_average", y="popularity",
            size="vote_count", color="revenue",
            hover_name="title",
            color_continuous_scale="Oranges",
            size_max=30,
        )
        fig4.update_layout(
            plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
            font_color="#e8e0d0",
            xaxis=dict(gridcolor="#1a1a2a"), yaxis=dict(gridcolor="#1a1a2a"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig4, width='stretch')


# Tab 3: Genre breakdown
with tab3:
    st.markdown("### Genre Breakdown")

    if len(df) > 0:
        g_sql, g_params = build_movie_query(
            year_range, selected_genres, selected_lang,
            min_rating, min_votes, include_adult, sort_by, result_limit
        )
        genre_sql = f"""
            SELECT g.genre_name, COUNT(*) AS count,
                   AVG(sub.vote_average) AS avg_rating,
                   SUM(sub.revenue) AS total_revenue
            FROM movie_genre mg
            JOIN genre g ON g.genre_id = mg.genre_id
            JOIN ({g_sql}) AS sub ON sub.movie_id = mg.movie_id
            GROUP BY g.genre_name
            ORDER BY count DESC
        """
        gdf = query(genre_sql, g_params)

        if not gdf.empty:
            gc1, gc2 = st.columns(2)
            with gc1:
                fig_g = px.bar(
                    gdf.head(15), x="count", y="genre_name", orientation="h",
                    color="avg_rating", color_continuous_scale="YlOrRd",
                    labels={"count": "Movies", "genre_name": ""},
                )
                fig_g.update_layout(
                    plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
                    font_color="#e8e0d0",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig_g, width='stretch')

            with gc2:
                fig_pie = px.pie(
                    gdf.head(10), names="genre_name", values="count",
                    color_discrete_sequence=px.colors.sequential.Oranges_r,
                )
                fig_pie.update_layout(
                    plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
                    font_color="#e8e0d0",
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig_pie, width='stretch')

# Tab 4: Language buckets
with tab4:
    st.markdown("### Language Distribution")

    l_sql, l_params = build_movie_query(
        year_range, selected_genres, selected_lang,
        min_rating, min_votes, include_adult, sort_by, result_limit
    )
    lang_sql = f"""
        SELECT l.language_name, l.language_code,
               COUNT(*) AS count,
               AVG(sub.vote_average) AS avg_rating
        FROM ({l_sql}) AS sub
        JOIN language l ON l.language_code = sub.original_language_code
        GROUP BY l.language_code, l.language_name
        ORDER BY count DESC
        LIMIT 20
    """
    ldf = query(lang_sql, l_params)

    if not ldf.empty:
        ldf["label"] = ldf["language_name"].fillna(ldf["language_code"])
        fig_l = px.bar(
            ldf, x="label", y="count",
            color="avg_rating", color_continuous_scale="YlOrRd",
            labels={"label": "Language", "count": "Movies"},
        )
        fig_l.update_layout(
            plot_bgcolor="#0a0a0f", paper_bgcolor="#0a0a0f",
            font_color="#e8e0d0",
            xaxis=dict(gridcolor="#1a1a2a"), yaxis=dict(gridcolor="#1a1a2a"),
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig_l, width='stretch')

with tab6:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Market Share by Production Company")
        countries_df = query("SELECT country_name FROM country ORDER BY country_name;")
        selected_country = st.selectbox("Select Country", options=countries_df['country_name'], index=0)
        top_n_market = st.slider("Show Top N Companies", 5, 20, 10)
        
        # Notice the added JOINs and WHERE clause to make the country filter work
        market_sql = f"""
            SELECT c.company_name, SUM(m.revenue) AS company_revenue
            FROM company c
            JOIN movie_company mc ON c.company_id = mc.company_id
            JOIN movie m ON mc.movie_id = m.movie_id
            JOIN movie_country m_loc ON m.movie_id = m_loc.movie_id
            JOIN country ct ON m_loc.country_id = ct.country_id
            WHERE m.budget > 1000000
                AND m.revenue > 0
                AND YEAR(m.release_date) BETWEEN {year_range[0]} AND {year_range[1]}
                AND ct.country_name = '{selected_country}'
            GROUP BY c.company_id, c.company_name
            ORDER BY company_revenue DESC
            LIMIT {top_n_market}
        """
        
        df_market = query(market_sql)
        
        if not df_market.empty:
            # Match pie slices to your cinematic theme using a warm gold/orange palette
            fig_pie = px.pie(
                df_market, 
                values='company_revenue', 
                names='company_name', 
                title=f"Top {top_n_market} Companies in {selected_country}", 
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.YlOrBr[::-1]
            )
            
            # Apply CSS styling directly to Plotly
            fig_pie.update_layout(
                paper_bgcolor='#0a0a0f',   
                plot_bgcolor='#0a0a0f',
                font=dict(family="DM Sans", color="#e8e0d0"),
                title=dict(
                    font=dict(family="Bebas Neue", size=24, color="#f5c518")
                ),
                legend=dict(
                    font=dict(color="#e8e0d0") # Forces the legend text to be light
                ),
                margin=dict(t=60, b=20, l=20, r=20)
            )
            
            # Note: use_container_width=True is preferred over width='stretch' in newer Streamlit versions
            st.plotly_chart(fig_pie, use_container_width=True)
        
        else:
            st.warning(f"No data for {selected_country}")

    with c2:
        st.markdown("### Market Share by Production Company")
         
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
        df_roi = query(roi_query)
        
        if not df_roi.empty:
            fig_bar = px.bar(
                df_roi, 
                x='roi_multiplier', 
                y='company_name', 
                orientation='h',
                title=f"ROI Leaders ({year_range[0]} - {year_range[1]})",
                color='roi_multiplier',
                # Swapped Viridis for a cinematic warm palette
                color_continuous_scale='YlOrBr', 
                hover_data={'total_investment': ':$,.0f', 'total_return': ':$,.0f'}
            )
        
            # Apply the cinematic theme layout updates
            fig_bar.update_layout(
                yaxis={'categoryorder':'total ascending'},
                paper_bgcolor='#0a0a0f',   
                plot_bgcolor='#0a0a0f',
                font=dict(family="DM Sans", color="#e8e0d0"),
                title=dict(
                    font=dict(family="Bebas Neue", size=24, color="#f5c518")
                ),
                # Style the legend/color bar text
                coloraxis_colorbar=dict(
                    title="ROI Multiplier",
                    title_font=dict(family="DM Sans", size=14, color="#e8e0d0"),
                    tickfont=dict(color="#e8e0d0")
                ),
                margin=dict(t=60, b=20, l=20, r=20)
            )
            
            # Use the modern Streamlit container width parameter
            st.plotly_chart(fig_bar, use_container_width=True)
        
        else:
            st.warning(f"No companies met the criteria for the years {year_range[0]}-{year_range[1]}.")

with tab8:
    st.title("📈 Genre Popularity & Momentum")
    st.write("Analyzing genre trends using 3-year moving averages.")
    with st.sidebar:
        st.header("Forecast Settings")
        all_genres = query("SELECT genre_name FROM genre ORDER BY genre_name")
        selected_genres = st.multiselect("Select Genres", 
                                     options=all_genres['genre_name'], 
                                     default=["Action", "Documentary"])

    if not selected_genres:
        st.warning("Select a genre to see the trend.")
        st.stop()

    genre_filter = "', '".join(selected_genres)

    # --- 2. SQL QUERIES ---

    # Query A: Historical 3-Year Smoothed Popularity
    hist_query = f"""
    SELECT genre_name, release_year, smoothed_pop
    FROM (
        SELECT 
            g.genre_name,
            YEAR(m.release_date) AS release_year,
            AVG(m.popularity) OVER (
                PARTITION BY g.genre_name ORDER BY YEAR(m.release_date) 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS smoothed_pop
        FROM genre g
        JOIN movie_genre mg ON g.genre_id = mg.genre_id
        JOIN movie m ON mg.movie_id = m.movie_id
        WHERE m.release_date IS NOT NULL AND g.genre_name IN ('{genre_filter}')
    ) AS t
    WHERE release_year >= 2000;
    """

    # Query B: Linear Regression Forecast for 2027-2028
    pred_query = f"""
    WITH HistoricalSmoothed AS (
        SELECT 
            g.genre_name, 
            YEAR(m.release_date) AS release_year,
            AVG(m.popularity) OVER (
                PARTITION BY g.genre_name ORDER BY YEAR(m.release_date) 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS smoothed_pop
        FROM genre g 
        JOIN movie_genre mg ON g.genre_id = mg.genre_id 
        JOIN movie m ON mg.movie_id = m.movie_id
        WHERE m.release_date IS NOT NULL AND g.genre_name IN ('{genre_filter}') AND m.popularity > 1
    ),
    Coefficients AS (
        SELECT 
            genre_name, 
            COUNT(*) AS n, 
            SUM(release_year) AS sum_x, 
            SUM(smoothed_pop) AS sum_y, 
            SUM(release_year * smoothed_pop) AS sum_xy, 
            SUM(release_year * release_year) AS sum_xx
        FROM HistoricalSmoothed GROUP BY genre_name
    ),
    Model AS (
        SELECT 
            genre_name, 
            ((n * sum_xy) - (sum_x * sum_y)) * 1.0 / NULLIF((n * sum_xx) - (sum_x * sum_x), 0) AS slope,
            (sum_y / n) - (((n * sum_xy) - (sum_x * sum_y)) * 1.0 / NULLIF((n * sum_xx) - (sum_x * sum_x), 0) * (sum_x / n)) AS intercept 
        FROM Coefficients
    )
    SELECT genre_name, 2027 AS year, ROUND(intercept + (slope * 2027), 2) AS popularity FROM Model
    UNION ALL
    SELECT genre_name, 2028 AS year, ROUND(intercept + (slope * 2028), 2) AS popularity FROM Model;
    """

    df_hist = query(hist_query)
    df_pred = query(pred_query)

    # Standardize Historical Columns
    df_hist.columns = ['genre_name', 'year', 'popularity']

    # --- 3. LINEAR GRAPH VISUALIZATION ---
    fig = go.Figure()
    colors = px.colors.qualitative.Plotly

    for i, genre in enumerate(selected_genres):
        h_genre = df_hist[df_hist['genre_name'] == genre].sort_values('year')
        p_genre = df_pred[df_pred['genre_name'] == genre].sort_values('year')
        color = colors[i % len(colors)]

        if not h_genre.empty:
            # Plot Historical Line
            fig.add_trace(go.Scatter(
                x=h_genre['year'], y=h_genre['popularity'].avg(),
                name=f"{genre} (Actual)", mode='lines+markers',
                line=dict(color=color, width=3)
            ))
            
            if not p_genre.empty:
                # Stitch the forecast to the last actual point
                bridge_x = [h_genre['year'].iloc[-1]] + p_genre['year'].tolist()
                bridge_y = [h_genre['popularity'].iloc[-1]] + p_genre['popularity'].tolist()
                
                # Plot Forecast Line
                fig.add_trace(go.Scatter(
                    x=bridge_x, y=bridge_y,
                    name=f"{genre} (Forecast)", mode='lines',
                    line=dict(color=color, width=3, dash='dash'),
                    showlegend=False
                ))

    fig.update_layout(
        title="<b>Genre Momentum: Historical Trends & 24-Month Prediction</b>",
        xaxis_title="Year", yaxis_title="Avg Popularity",
        hovermode="x unified", template="plotly_white"
    )

    st.plotly_chart(fig, use_container_width=True)