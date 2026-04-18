# 📊 GCash Reviews Pipeline - Understanding User Sentiment from Google Play Reviews

#### 🧠 Story Time (Context)

So during my initial project scoping process, I was initially targeting on how gambling has has evolved into a growing concern in the Philippines over the past two years. Gambling has gone so bad that influencers are throwing aways their morals just to promote gambling in which most of the people attracted to this are the lower class in which does not help. Unfortunately gambling apps on Google Play Store so I cannot extract reviews/sentiments on how people view such gambling apps/platforms.

I shifted my focus to GCash because of this one interaction with my mom:

> *"Anak tulugan mo nga ako mag-login sa GCash at nakailang OTP na ako di parin ako makapasok sa GCash" (Tagalog/Philippines*)
>
>"Son, can you help me login to my GCash, I’ve tried multiple OTPs and still can’t access my account."

Based on this interaction this made me think, that are other users of GCash also experiencing this? This prompted me to shift my initial target of gambling platforms to GCash and take a look if others are experiencing isses as well.


## Problem Statement

<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/5/52/GCash_logo.svg/500px-GCash_logo.svg.png" alt="GCash Logo" width="220"/>
</p>

<h1 align="center">
GCash Reviews Pipeline
</h1>

<p align="center">
Understanding User Sentiment from Google Play Reviews
</p>

User reviews from GCash are a goldmine of opinions, people talking about bugs, frustrations, good experiences, and everything in between. The problem is, these reviews are just walls of unstructured text, making it hard to actually see patterns or get a clear sense of how users feel overall.

Scrolling through thousands of reviews manually isn’t practical, and it’s easy to miss trends—like when negative feedback suddenly spikes or when the same issue keeps showing up.

So instead of reading reviews one by one, this project builds an end-to-end data pipeline that automatically collects, processes, and analyzes GCash app reviews from the Google Play Store.

#### 🔄 Pipeline Overview

- Extract review data using [google-play-scraper](https://pypi.org/project/google-play-scraper/)
- Transform raw review text through cleaning and preprocessing
- Enrich each review using sentiment analysis to classify it as:
  - Positive
  - Neutral
  - Negative
- Assign issue categories to each review (e.g., login/OTP issues, app crashes, transaction delays, UI/UX problems) to group similar complaints together
- Load the processed data into a cloud data warehouse
- Serve the data for reporting and visualization of sentiment trends over time

The objective is to create a scalable system that converts raw user feedback into structured analytical data, allowing stakeholders to better understand how users perceive the application and quickly detect shifts in customer sentiment.


## 🏗️ Project Archietcture

![GCash Reviews Pipeline](resources/diagrams/gcash-reviews-pipeline_withbg.gif)

- **Infrastructure:** Terraform (GCP provisioning)

- **Ingestion (Batch Pipeline):**
  - Google Play Store scraping using [google-play-scraper](https://pypi.org/project/google-play-scraper/)
  - Raw reviews extracted as JSON

- **Data Lake (Medallion Architecture - GCS):**
  - **Bronze Layer:** Raw JSON storage
  - **Silver Layer:** Cleaned and enriched datasets
    - Text cleaning & preprocessing
    - Sentiment classification (Positive / Neutral / Negative)
    - Issue categorization & clustering (e.g., OTP, login issues, crashes)

- **Data Warehouse:**
  - BigQuery (primary analytical store)
  - DuckDB (local development / testing fallback)

- **Transformations (Gold Layer):**
  - dbt-based modeling inside BigQuery
  - Aggregations (sentiment trends, issue frequency, time-based analysis)
  - Creation of analytics-ready tables

- **Orchestration:**
  - Kestra (pipeline scheduling and workflow management across ingestion → processing → dbt runs)

- **Serving Layer:**
  - Streamlit dashboard for:
    - Sentiment trends over time
    - Top issue categories
    - Review clustering insights

- **Testing & Validation:**
  - Data quality checks (schema + null validation)
  - Sentiment classification validation
  - dbt transformation testing and consistency checks


#### Checklist
- [ ] Terraform Provision
- [ ] Ingestion > Batch - Using google-play-scraper = json
- [ ] Data Lake > Raw and Processed
- [ ] Processing > parse, clean, issue assignment, sentiment assignment and transforms
- [ ] GCP > buckets and bigquery
- [ ] Warehouse > local (duckdb fallback) and GCP
- [ ] dbt > Transforms
- [ ] orchestrate > Kestra
- [ ] dashboard > streamlit
- [ ] test > sentiments and transform in processing
- [ ] documentation flowcharts.