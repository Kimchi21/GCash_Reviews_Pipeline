import os
import duckdb
import pandas as pda
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

### page config ###

st.set_page_config(
    page_title="GCash Reviews Dashboard",
    page_icon="📱",
    layout="wide",
)

### data loading ###

@st.cache_data(ttl=3600)
def load_from_bigquery():
    credentials = service_account.Credentials.from_service_account_file(
        os.environ["gcp_service_account"]
    )
    client = bigquery.Client(
        project=os.environ["GCP_PROJECT_ID"],
        credentials=credentials,
    )

    monthly = client.query("""
        SELECT * FROM `gcash-reviews-pipeline.gcash_reviews_gold_gold.monthly_ratings`
        ORDER BY year_month
    """).to_dataframe()

    sentiment = client.query("""
        SELECT * FROM `gcash-reviews-pipeline.gcash_reviews_gold_gold.sentiment_summary`
        ORDER BY year_month
    """).to_dataframe()

    category = client.query("""
        SELECT * FROM `gcash-reviews-pipeline.gcash_reviews_gold_gold.category_trends`
        ORDER BY year_month
    """).to_dataframe()

    version = client.query("""
        SELECT * FROM `gcash-reviews-pipeline.gcash_reviews_gold_gold.version_ratings`
        ORDER BY avg_rating DESC
    """).to_dataframe()

    return monthly, sentiment, category, version


@st.cache_data(ttl=3600)
def load_from_duckdb():
    con = duckdb.connect()
    parquet_path = "notebooks/gcash_reviews.parquet"

    monthly = con.execute(f"""
        SELECT
            strftime(reviewed_at, '%Y-%m')             as year_month,
            extract('year' from reviewed_at)            as review_year,
            extract('month' from reviewed_at)           as review_month,
            count(*)                                    as total_reviews,
            round(avg(score), 2)                        as avg_rating,
            sum(case when score = 5 then 1 else 0 end)  as five_star,
            sum(case when score = 4 then 1 else 0 end)  as four_star,
            sum(case when score = 3 then 1 else 0 end)  as three_star,
            sum(case when score = 2 then 1 else 0 end)  as two_star,
            sum(case when score = 1 then 1 else 0 end)  as one_star,
            sum(case when sentiment = 'positive' then 1 else 0 end) as positive_count,
            sum(case when sentiment = 'negative' then 1 else 0 end) as negative_count,
            sum(case when sentiment = 'neutral'  then 1 else 0 end) as neutral_count
        FROM read_parquet('{parquet_path}')
        GROUP BY 1, 2, 3
        ORDER BY 1
    """).df()

    sentiment = con.execute(f"""
        SELECT
            strftime(reviewed_at, '%Y-%m')  as year_month,
            extract('year' from reviewed_at) as review_year,
            extract('month' from reviewed_at) as review_month,
            sentiment,
            count(*)                         as review_count,
            round(avg(score), 2)             as avg_score
        FROM read_parquet('{parquet_path}')
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 4
    """).df()

    category = con.execute(f"""
        SELECT
            strftime(reviewed_at, '%Y-%m')  as year_month,
            extract('year' from reviewed_at) as review_year,
            extract('month' from reviewed_at) as review_month,
            category,
            sentiment,
            count(*)                         as review_count,
            round(avg(score), 2)             as avg_score,
            sum(thumbs_up)                   as total_thumbs_up
        FROM read_parquet('{parquet_path}')
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1, 4, 5
    """).df()

    version = con.execute(f"""
        SELECT
            app_version,
            count(*)                                    as total_reviews,
            round(avg(score), 2)                        as avg_rating,
            sum(case when sentiment = 'positive' then 1 else 0 end) as positive_count,
            sum(case when sentiment = 'negative' then 1 else 0 end) as negative_count,
            sum(case when sentiment = 'neutral'  then 1 else 0 end) as neutral_count,
            round(sum(case when sentiment = 'positive' then 1 else 0 end) * 100.0 / count(*), 2) as positive_pct,
            round(sum(case when sentiment = 'negative' then 1 else 0 end) * 100.0 / count(*), 2) as negative_pct,
            min(reviewed_at) as first_review_date,
            max(reviewed_at) as last_review_date
        FROM read_parquet('{parquet_path}')
        WHERE app_version IS NOT NULL
        GROUP BY 1
        HAVING count(*) >= 10
        ORDER BY avg_rating DESC
    """).df()

    con.close()
    return monthly, sentiment, category, version


@st.cache_data(ttl=3600)
def load_review_text():
    """Load raw review content for wordcloud — from parquet only."""
    con = duckdb.connect()
    parquet_path = "notebooks/gcash_reviews.parquet"
    df = con.execute(f"""
        SELECT content
        FROM read_parquet('{parquet_path}')
        WHERE content IS NOT NULL AND content != ''
    """).df()
    con.close()
    return " ".join(df["content"].tolist())


### sidebar ###

st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/5/52/GCash_logo.svg/500px-GCash_logo.svg.png",
)

st.sidebar.markdown("""
### 📊 GCash App Reviews Dashboard

Analyze user feedback from the Google Play Store to understand how the GCash app is performing over time.

Dive into:
- ⭐ Rating distributions and trends  
- 💬 Sentiment analysis (positive, neutral, negative)  
- 🧩 Common issue categories and user concerns  
- 📱 App version performance and release impact
""")

st.sidebar.divider()

# data source toggle
use_local = st.sidebar.toggle(
    "Use local DuckDB",
    value=False,
    help="Switch to local parquet file instead of BigQuery"
)

### load data ###

if use_local:
    (monthly_df, sentiment_df, category_df, version_df) = load_from_duckdb()
    data_source = "DuckDB (local)"
else:
    try:
        (monthly_df, sentiment_df, category_df, version_df) = load_from_bigquery()
        data_source = "BigQuery"
    except Exception as e:
        st.warning(f"BigQuery unavailable ({e}) — falling back to local DuckDB")
        (monthly_df, sentiment_df, category_df, version_df) = load_from_duckdb()
        data_source = "DuckDB (local)"

st.sidebar.caption(f"Data source: {data_source}")
st.sidebar.divider()
st.sidebar.header("Filters")

### sidebar filters ###

all_months = sorted(monthly_df["year_month"].unique())
min_month, max_month = st.sidebar.select_slider(
    "Date range",
    options=all_months,
    value=(all_months[0], all_months[-1]),
)

all_categories = sorted(category_df["category"].unique())
selected_categories = st.sidebar.multiselect(
    "Categories",
    options=all_categories,
    default=all_categories,
)

all_sentiments = ["positive", "neutral", "negative"]
selected_sentiments = st.sidebar.multiselect(
    "Sentiment",
    options=all_sentiments,
    default=all_sentiments,
)

all_versions = sorted(version_df["app_version"].unique())
selected_versions = st.sidebar.multiselect(
    "App versions",
    options=all_versions,
    default=[],
    placeholder="All versions",
)

st.sidebar.divider()

st.sidebar.title("🤝 Connect With Me")

st.sidebar.markdown("""
- LinkedIn: [Kim Marcial A. Vallesteros](https://www.linkedin.com/in/kimmarcialvallesteros/)
- email: [kimmarcialv@gmail.com](mailto:kimmarcialv@gmail.com)
""")

### Add profile link


### apply filters ###

monthly_filtered = monthly_df[
    (monthly_df["year_month"] >= min_month) &
    (monthly_df["year_month"] <= max_month)
]

sentiment_filtered = sentiment_df[
    (sentiment_df["year_month"] >= min_month) &
    (sentiment_df["year_month"] <= max_month) &
    (sentiment_df["sentiment"].isin(selected_sentiments))
]

category_filtered = category_df[
    (category_df["year_month"] >= min_month) &
    (category_df["year_month"] <= max_month) &
    (category_df["category"].isin(selected_categories)) &
    (category_df["sentiment"].isin(selected_sentiments))
]

version_filtered = version_df.copy()
if selected_versions:
    version_filtered = version_filtered[
        version_filtered["app_version"].isin(selected_versions)
    ]

sentiment_colors = {
    "positive": "#2ecc71",
    "neutral":  "#95a5a6",
    "negative": "#e74c3c",
}

### header ###

st.title("📱 GCash App Reviews Dashboard")
st.divider()

### kpi cards ###

st.subheader("Overview")
k1, k2, k3, k4, k5 = st.columns(5)

total_reviews = int(monthly_filtered["total_reviews"].sum())
avg_rating    = round(monthly_filtered["avg_rating"].mean(), 2)
total_pos     = int(monthly_filtered["positive_count"].sum())
total_neg     = int(monthly_filtered["negative_count"].sum())
positive_pct  = round(total_pos / total_reviews * 100, 1) if total_reviews else 0

k1.metric("Total reviews",  f"{total_reviews:,}",border=True)
k2.metric("Avg rating",     f"{avg_rating} ⭐",border=True)
k3.metric("Positive",       f"{total_pos:,}",border=True)
k4.metric("Negative",       f"{total_neg:,}",border=True)
k5.metric("Positive rate",  f"{positive_pct}%",border=True)

st.divider()

### section 1: volume & ratings ###

st.subheader("📈 Volume & ratings")

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        monthly_filtered,
        x="year_month",
        y="total_reviews",
        title="Monthly review volume",
        color_discrete_sequence=["#4F8BF9"],
    )
    fig.update_layout(xaxis_title="Month", yaxis_title="Reviews")
    st.plotly_chart(fig, width="stretch")

with col2:
    fig = px.line(
        monthly_filtered,
        x="year_month",
        y="avg_rating",
        title="Average rating over time",
        markers=True,
        color_discrete_sequence=["#F9A84F"],
    )
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Avg rating",
        yaxis=dict(range=[1, 5])
    )
    st.plotly_chart(fig, width="stretch")

st.divider()

### section 2: sentiment ###

st.subheader("💬 Sentiment analysis")

fig = px.area(
    sentiment_filtered,
    x="year_month",
    y="review_count",
    color="sentiment",
    title="Sentiment distribution over time",
    color_discrete_map=sentiment_colors,
)
fig.update_layout(xaxis_title="Month", yaxis_title="Reviews")
st.plotly_chart(fig, width="stretch")

st.divider()

### section 3: categories ###

st.subheader("🗂️ Issue categories")

col1, col2 = st.columns(2)

with col1:
    cat_totals = (
        category_filtered
        .groupby("category")["review_count"]
        .sum()
        .reset_index()
        .sort_values("review_count", ascending=False)
    )
    fig = px.bar(
        cat_totals,
        x="review_count",
        y="category",
        orientation="h",
        title="Reviews by category",
        color="review_count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(xaxis_title="Reviews", yaxis_title="Category")
    st.plotly_chart(fig, width="stretch")

with col2:
    fig = px.pie(
        cat_totals,
        values="review_count",
        names="category",
        title="Category share",
        hole=0.4,
    )
    st.plotly_chart(fig, width="stretch")

cat_time = (
    category_filtered
    .groupby(["year_month", "category"])["review_count"]
    .sum()
    .reset_index()
)
fig = px.line(
    cat_time,
    x="year_month",
    y="review_count",
    color="category",
    title="Category trends over time",
    markers=False,
)
fig.update_layout(xaxis_title="Month", yaxis_title="Reviews")
st.plotly_chart(fig, width="stretch")

cat_sentiment = (
    category_filtered
    .groupby(["category", "sentiment"])["review_count"]
    .sum()
    .reset_index()
)
fig = px.bar(
    cat_sentiment,
    x="category",
    y="review_count",
    color="sentiment",
    title="Sentiment breakdown by category",
    color_discrete_map=sentiment_colors,
    barmode="stack",
)
fig.update_layout(xaxis_title="Category", yaxis_title="Reviews")
st.plotly_chart(fig, width="stretch")

st.divider()

### section 4: app versions ###

st.subheader("📦 App version analysis")

col1, col2 = st.columns(2)

with col1:
    top_versions = version_filtered.head(20)
    fig = px.bar(
        top_versions,
        x="avg_rating",
        y="app_version",
        orientation="h",
        title="Top 20 versions by avg rating",
        color="avg_rating",
        color_continuous_scale="RdYlGn",
    )
    fig.update_layout(xaxis_title="Avg rating", yaxis_title="Version")
    st.plotly_chart(fig, width="stretch")

with col2:
    top_by_volume = version_filtered.nlargest(20, "total_reviews")
    fig = px.bar(
        top_by_volume,
        x="total_reviews",
        y="app_version",
        orientation="h",
        title="Top 20 versions by review volume",
        color="total_reviews",
        color_continuous_scale="Blues",
    )
    fig.update_layout(xaxis_title="Total reviews", yaxis_title="Version")
    st.plotly_chart(fig, width="stretch")

top15 = version_filtered.nlargest(15, "total_reviews")
version_sentiment = top15.melt(
    id_vars=["app_version"],
    value_vars=["positive_count", "negative_count", "neutral_count"],
    var_name="sentiment",
    value_name="count",
)
version_sentiment["sentiment"] = version_sentiment["sentiment"].str.replace("_count", "")
fig = px.bar(
    version_sentiment,
    x="app_version",
    y="count",
    color="sentiment",
    title="Sentiment breakdown — top 15 versions by volume",
    color_discrete_map=sentiment_colors,
    barmode="stack",
)
fig.update_layout(xaxis_title="Version", yaxis_title="Reviews")
st.plotly_chart(fig, width="stretch")

preferred_cols = [
    "app_version", "total_reviews", "avg_rating",
    "positive_pct", "negative_pct",
    "first_review_date", "last_review_date"
]
available_cols = [c for c in preferred_cols if c in version_filtered.columns]
st.subheader("Version details")
st.dataframe(
    version_filtered[available_cols].reset_index(drop=True),
    width="stretch",
)

st.divider()

### section 5: wordcloud ###

st.subheader("☁️ Review wordcloud")
st.caption("Most frequently used words across all reviews")

with st.spinner("Generating wordcloud..."):
    text = load_review_text()
    wc = WordCloud(
        width=1400,
        height=600,
        background_color="white",
        max_words=200,
        colormap="Blues",
        collocations=False,
    ).generate(text)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)