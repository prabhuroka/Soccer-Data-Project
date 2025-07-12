import sqlite3
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import adapt, register_adapter
import numpy as np
from tqdm import tqdm

# Handle numpy types in psycopg2
def adapt_numpy_float64(numpy_float):
    return adapt(float(numpy_float))
register_adapter(np.float64, adapt_numpy_float64)

def adapt_numpy_int64(numpy_int):
    try:
        return adapt(int(numpy_int))
    except (OverflowError, ValueError):
        return adapt(str(numpy_int))  # Store as text if too large
register_adapter(np.int64, adapt_numpy_int64)

def extract_sqlite_data():
    """Extract data from SQLite database"""
    try:
        sqlite_db = "data/database.sqlite"
        conn = sqlite3.connect(sqlite_db)
        
        print("Extracting data from SQLite...")
        
        # Extract all data
        matches_df = pd.read_sql("SELECT * FROM Match;", conn)
        players_df = pd.read_sql("SELECT * FROM Player;", conn)
        teams_df = pd.read_sql("SELECT * FROM Team;", conn)
        leagues_df = pd.read_sql("SELECT * FROM League;", conn)
        
        conn.close()
        print("Data extracted successfully!")
        return matches_df, players_df, teams_df, leagues_df
        
    except Exception as e:
        print(f"Error extracting data: {e}")
        raise

def create_pg_tables(pg_conn):
    """Create tables in PostgreSQL with flexible numeric types"""
    try:
        cur = pg_conn.cursor()
        
        # Create matches table with NUMERIC for extremely large numbers
        cur.execute("""
        CREATE TABLE matches (
            id SERIAL PRIMARY KEY,
            country_id INTEGER,
            league_id INTEGER,
            season TEXT,
            stage INTEGER,
            date TEXT,
            match_api_id NUMERIC,  -- Changed to NUMERIC
            home_team_api_id NUMERIC,
            away_team_api_id NUMERIC,
            home_team_goal INTEGER,
            away_team_goal INTEGER,
            home_player_1 NUMERIC,
            home_player_2 NUMERIC,
            home_player_3 NUMERIC,
            home_player_4 NUMERIC,
            home_player_5 NUMERIC,
            home_player_6 NUMERIC,
            home_player_7 NUMERIC,
            home_player_8 NUMERIC,
            home_player_9 NUMERIC,
            home_player_10 NUMERIC,
            home_player_11 NUMERIC,
            away_player_1 NUMERIC,
            away_player_2 NUMERIC,
            away_player_3 NUMERIC,
            away_player_4 NUMERIC,
            away_player_5 NUMERIC,
            away_player_6 NUMERIC,
            away_player_7 NUMERIC,
            away_player_8 NUMERIC,
            away_player_9 NUMERIC,
            away_player_10 NUMERIC,
            away_player_11 NUMERIC,
            goal TEXT,
            shoton TEXT,
            shotoff TEXT,
            foulcommit TEXT,
            card TEXT,
            "cross" TEXT,
            corner TEXT,
            possession TEXT
        );
        """)
        
        # Players table
        cur.execute("""
        CREATE TABLE players (
            id SERIAL PRIMARY KEY,
            player_api_id NUMERIC,
            player_name TEXT,
            player_fifa_api_id NUMERIC,
            birthday TEXT,
            height FLOAT,
            weight INTEGER
        );
        """)
        
        # Teams table
        cur.execute("""
        CREATE TABLE teams (
            id SERIAL PRIMARY KEY,
            team_api_id NUMERIC,
            team_fifa_api_id NUMERIC,
            team_long_name TEXT,
            team_short_name TEXT
        );
        """)
        
        # Leagues table
        cur.execute("""
        CREATE TABLE leagues (
            id SERIAL PRIMARY KEY,
            country_id INTEGER,
            name TEXT
        );
        """)
        
        pg_conn.commit()
        print("✔ Tables created successfully")
        
    except Exception as e:
        pg_conn.rollback()
        print(f"Error creating tables: {e}")
        raise

def safe_convert(value):
    """Convert values safely for PostgreSQL"""
    if pd.isna(value):
        return None
    if isinstance(value, (np.int64, np.int32)):
        try:
            return int(value)
        except (OverflowError, ValueError):
            return str(value)
    return value

def insert_data(pg_conn, table_name, df):
    """Insert data into PostgreSQL with row-by-row error handling"""
    try:
        cur = pg_conn.cursor()
        
        # Get table columns from PostgreSQL
        cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        """)
        table_columns = [row[0] for row in cur.fetchall()]
        
        # Filter DataFrame columns to match table columns
        df_columns = [col for col in df.columns if col in table_columns]
        
        if not df_columns:
            raise ValueError(f"No matching columns between DataFrame and table {table_name}")
        
        # Create SQL query
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, df_columns)),
            sql.SQL(', ').join([sql.Placeholder()] * len(df_columns))
        )
        
        # Insert row by row with error handling
        print(f"Loading {table_name} table ({len(df)} rows)...")
        success_count = 0
        error_count = 0
        
        for _, row in tqdm(df[df_columns].iterrows(), total=len(df)):
            try:
                # Convert each value safely
                values = [safe_convert(row[col]) for col in df_columns]
                cur.execute(query, values)
                success_count += 1
            except Exception as e:
                error_count += 1
                # Log first few errors
                if error_count <= 5:
                    print(f"Error inserting row {_}: {e}")
                continue
        
        pg_conn.commit()
        print(f"✔ Successfully loaded {success_count} rows into {table_name}")
        if error_count > 0:
            print(f"⚠ Failed to load {error_count} rows")
        
    except Exception as e:
        pg_conn.rollback()
        print(f"Error inserting into {table_name}: {e}")
        raise

def load_to_postgres():
    """Main function to load data into PostgreSQL"""
    pg_conn = None
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host="localhost",
            database="soccer_db",
            user="postgres",
            password="password",
            connect_timeout=10
        )
        print("✔ PostgreSQL connection successful")
        
        # Extract data
        matches_df, players_df, teams_df, leagues_df = extract_sqlite_data()
        
        # Create tables
        create_pg_tables(pg_conn)
        
        # Insert data
        insert_data(pg_conn, "matches", matches_df)
        insert_data(pg_conn, "players", players_df)
        insert_data(pg_conn, "teams", teams_df)
        insert_data(pg_conn, "leagues", leagues_df)
        
        print("✅ All data loaded successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    load_to_postgres()