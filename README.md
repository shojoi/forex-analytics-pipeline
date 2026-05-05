# Forex Rate Analytics Pipeline

---

# Data Pipeline Overview

This project demonstrates a modern data pipeline for ingesting and transforming foreign exchage (Forex) exchange rate data to enable time-series analytics and business insights. Custom Connector created on Airbyte Cloud for the source API handles ingestion into Snowflake data warehouse on a fixed schedule, while Dagster orchestrates dbt transformations of raw data into analytical datasets for end-user reporting.

----

# Consumers

Data Analysts, Business Analysts, Finance Teams who want to analyze currency trends and performance, perform time-series analysis, identify strongest/weakest performing currencies, monitor forex rate fluctuations to support decision-making for international transactions.

-----

# Business Questions

<img width="1438" height="417" alt="Screenshot 2026-05-05 120903" src="https://github.com/user-attachments/assets/d7d915b0-c218-4a28-85f6-258c6ae20750" />


------

# Source Dataset

Forex rate data is sourced from <a href="https://currencybeacon.com/api-documentation" target="_blank">Currency Beacon API</a>

| Dataset | Source name| Description |
|----------|----------|----------|
| Latest Exchange Rates  | latest  | Retrieve real-time exchange rate data for all available currencies.    |
| Currency conversion  | currencies | The convert endpoint allows you to convert any amount from one currency to another using real-time mid-market exchange rates.   |
| Historical rates  | historical  | Retrieve historical exchange rates for any specific date in the past  |
| Time series rates  | timeseries  | The time-series endpoint returns daily historical exchange rates between two specified dates.   |


# Solution Architecture

<img width="1552" height="831" alt="Screenshot 2026-05-04 184309" src="https://github.com/user-attachments/assets/681ceecf-0ad0-4515-9216-68f0a1acb600" />

# Project Structure

<img width="1071" height="734" alt="Screenshot 2026-05-05 124355" src="https://github.com/user-attachments/assets/c1f6a175-154e-416d-a2a3-973009396c0f" />

# Getting Started 

##  Prerequisites

- **Airbyte Cloud** – for ingesting API data  
- **Snowflake** – for raw + transformed layers  
- **Dagster Cloud** – for orchestrating dbt and assets
- **Preset account** - for Dashboard
- **Currency Beacon API Key** - for forex data extraction
- **dbt Core** – for local development and testing
- **GitHub account** – for version control and CI/CD
- **Python 3.10+** (up to Python 3.13) – for local development


   ## Steps for Execution

## **1. Airbyte Setup**  
   Configure the Currency Beacon API source and create a connection that syncs daily at 12 AM.

## **2. Snowflake Setup**  
   Create the required database, schemas (RAW, STAGING, CORE, MARTS), and grant permissions.

## **3. Clone Repository & Environment Setup**

### **Clone the repository**
```bash
git clone https://github.com/shojoi/forex-analytics-pipeline.git
cd forex-analytics-pipeline
```

---

### **Create and activate a Python environment**  
(Use either **conda** or **venv**)

#### **Using conda**
```bash
conda create -n forex_pipeline python=3.11
conda activate forex_pipeline
```

#### **Using venv**
```bash
python -m venv venv
source venv/bin/activate   # macOS/Linux
venv\Scripts\activate      # Windows
```

---

### **Install dependencies**
```bash
pip install -r requirements.txt
```

Optional (if using editable local package structure):
```bash
pip install -e .
```

---

### **Configure environment variables**  
Create a `.env` file in the project root:

```
# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_WAREHOUSE=

# dbt
DBT_PROFILES_DIR=./data_transformation
```

### **Runing Locally**

#### **Run dbt manually**
```bash
cd data_transformation
dbt deps
dbt run
dbt test
```

#### **Start the Dagster development server**
```bash
dagster dev
```
Then open Dagster UI at http://localhost:3000

### **Run dbt assets through Dagster**
In the Dagster UI:

1. Go to **Assets**  
2. Select your dbt assets  
3. Click **Materialize All**  
4. Confirm that Snowflake tables update in the STAGING → CORE → MARTS layers 

---

## **4. Dagster Cloud Setup**


---

## **5. Preset Dashboard Setup**

### **Connect Preset to Snowflake**
In Preset:

1. Go to **Datasets → + Database**  
2. Choose **Snowflake**  
3. Enter your Snowflake credentials  
4. Test connection and save  

### **Import marts tables**
Use tables from the **MARTS** schema, such as:

- `mart_daily_rates `
- `mart_rolling_averages `
- `mart_monthly_snapshots`
- `mart_currency_strength_index`
- `mart_rate_changes `


### **Build dashboards**
Create visualizations such as:

- Daily FX rate trends  
- Currency pair comparisons  
- Volatility and movement analysis  
- Rolling averages and indicators  

Preset dashboards will automatically refresh as dbt updates the marts layer.

---




