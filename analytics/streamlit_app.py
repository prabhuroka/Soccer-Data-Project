import streamlit as st
import pandas as pd
import psycopg2  # Direct PostgreSQL adapter
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configuration
DB_CONFIG = {
    "host": "localhost",
    "database": "soccer_db",
    "user": "postgres",
    "password": "password"
}


@st.cache_resource
def get_db_connection():
    """Get a direct psycopg2 connection for pandas"""
    return psycopg2.connect(**DB_CONFIG)


@st.cache_resource
def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for other operations"""
    connection_url = URL.create(
        drivername="postgresql",
        **DB_CONFIG
    )
    return create_engine(connection_url)


def main():
    st.title("⚽ Soccer Analytics Dashboard")

    try:
        # Top Players Section
        st.header("Top 50 Players by Goals")
        with get_db_connection() as conn:
            top_players = pd.read_sql(
                "SELECT * FROM analytics.player_performance ORDER BY total_goals DESC LIMIT 50",
                conn
            )
        st.dataframe(top_players)

        # Team Performance
        st.header("Team Performance")
        with get_db_connection() as conn:
            team_stats = pd.read_sql(
                """SELECT team_name, wins, draws, 
                          total_goals_scored, total_goals_conceded
                   FROM analytics.team_analytics 
                   ORDER BY wins DESC""",
                conn
            )
        st.bar_chart(team_stats.set_index("team_name")[["wins", "draws"]])

    except Exception as e:
        st.error(f"Database error: {str(e)}")
        st.info("Please check if:")
        st.info("- Database is running")
        st.info("- Tables exist in the analytics schema")
        st.info("- Connection credentials are correct")


if __name__ == "__main__":
    main()