# ⚽ Soccer Analytics Data Pipeline

## 📌 Overview

A complete data engineering pipeline that processes soccer statistics from SQLite to PostgreSQL, transforms the data with dbt, orchestrates with Airflow, and visualizes with Streamlit.


## 🌟 Key Features

**ETL Pipeline**: Extracts data from SQLite and loads into PostgreSQL
**Data Transformation**: Uses dbt for analytics-ready tables
**Workflow Orchestration**: Managed by Apache Airflow
**Interactive Dashboard**: Streamlit web app for visualization
**Automated Processing**: Daily updates of player/team statistics

## 🛠️ Tech Stack

| Component        | Technology |
|-----------------|------------|
| Data Storage    | PostgreSQL |
| ETL             | Python (Pandas, SQLAlchemy) |
| Transformation  | dbt |
| Orchestration   | Apache Airflow |
| Visualization   | Streamlit |

## 📂 Project Structure
soccer_analytics/
├── data/ # Raw SQLite database
├── etl/ # Extraction and loading scripts
├── dbt/ # Data transformation models
├── airflow/ # Workflow orchestration
├── analytics/ # Streamlit dashboard
└── README.md


## 📊 Data Dictionary

### Data Source:  https://www.kaggle.com/datasets/hugomathien/soccer?resource=download

### Core Tables

| Table          | Description |
|---------------|-------------|
| `matches`     | Match results and player lineups |
| `players`     | Player attributes and metadata |
| `teams`       | Team information |
| `leagues`     | League details |

### Analytics Views

| View                   | Description |
|------------------------|-------------|
| `player_performance`   | Goals, matches played, and performance metrics |
| `team_analytics`       | Wins, losses, goals scored/conceded |




## 🚀 Installation Guide

### Prerequisites
- Python 3.11
- PostgreSQL 15+
- Git

### 1. Clone the Repository 

https://www.kaggle.com/datasets/hugomathien/soccer?resource=download Download dataset from kaggle and keep it in "/soccer_analytics/data/"


### 2.Set Up Virtual Environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate   # Windows

### 3. Install Dependencies
pip install -r requirements.txt

OR 

pip install pandas sqlalchemy psycopg2-binary apache-airflow dbt-postgres

### 4. Database Setup
# Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE soccer_db;"

OR (Manually from PostGres)
CREATE DATABASE soccer_db;
CREATE USER postgres WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE soccer_db TO postgres;

# Run ETL pipeline
python etl/extract_load.py

### 5. Transform Data with dbt
cd dbt
dbt run
cd ..

### 6. Start Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

airflow webserver --port 8080 & airflow scheduler

### 7. Launch Dashboard
streamlit run analytics/streamlit_app.py
