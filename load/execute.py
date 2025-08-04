import sys
import time
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time

def create_spark_session():
    """intialize a Spark session."""
    return SparkSession.builder \
        .appName("Load and Execute") \
        .config("spark.jars", "/Users/najibthapa1/Documents/College/bigdata/pyspark/venv/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.7.jar") \
        .getOrCreate()
def create_postgres_tables(pg_un, pg_pw):
    """Create PostgresSQL tables  if they don't exist using psycopg2."""
    conn = None
    try:
        conn  = psycopg2.connect(
            dbname = "postgres",
            user = pg_un,
            password = pg_pw,
            host = "localhost",
            port = "5432"
            )
        cursor = conn.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followers FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_ids TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50),
                related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                id VARCHAR(50),
                artist_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS tracks_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artists_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.debug("PostgreSQL tables created successfully.")

    except Exception as e:
        logger.warning(f"Error creating tables:{e}")

    finally:
        logger.debug("Closing connection and cursor to postgres db")
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir,pg_un, pg_pw):
    """Load Parquet files to PostgresSql."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded","recommendations_exploded"),
        ("stage3/artist_track","artist_track"),
        ("stage3/tracks_metadata","tracks_metadata"),
        ("stage3/artists_metadata","artists_metadata")
    ]

    for parquet_path, table_name in tables:
        try:
            df=spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if 'master' in parquet_path else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(jdbc_url, table_name, properties=connection_properties)
            logger.info(f"Loaded {table_name} to PostgresSQL")
        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")



if __name__ == "__main__":
    logger = setup_logging("load.log")
    if len(sys.argv) != 4:
        logger.error("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist.")
        sys.exit(1)
    logger.infor("Load stage started")
    start = time.time()
    spark = create_spark_session()
    create_postgres_tables(pg_un, pg_pw)
    load_to_postgres(spark, input_dir, pg_un, pg_pw)
    
    end = time.time()
    logger.info("Load stage completed")
    logger.info(f"Total time taken: {format_time(end-start)}")
