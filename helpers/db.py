import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

def connect_to_postgres():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT'),
            sslmode=os.getenv('DB_SSLMODE')
        )
        print("Successfully connected to PostgreSQL!")
        return conn
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

def get_pg_listings_ids(conn, batch_size=100000):
    """
    Get all listings IDs from PostgreSQL using a server-side cursor
    and yield results in batches
    """
    with conn.cursor(name='fetch_listings_ids') as cursor:
        cursor.itersize = batch_size
        cursor.execute("SELECT id FROM listings")
        
        while True:
            rows = cursor.fetchmany(size=batch_size)
            if not rows:
                break
            yield {row[0] for row in rows}  # Assuming id is the first (and only) column


def count_pg_listings(conn):
    """Count the total number of listings in PostgreSQL"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM listings")
        return cursor.fetchone()[0]


def fetch_listings_data(conn, id_batch, listings_table="listings", embeddings_table="listings_embeddings"):
    """
    Fetch full data for a batch of listing IDs
    
    Args:
    conn: PostgreSQL connection object
    id_batch: List of IDs to fetch
    listings_table: Name of the listings table (default: "listings")
    embeddings_table: Name of the embeddings table (default: "listing_embedding")
    """
    placeholders = ','.join(['%s'] * len(id_batch))
    query = f"""
    SELECT l.*, le.front_image_embeddings
    FROM {listings_table} l
    LEFT JOIN {embeddings_table} le ON l.id = le.listings_id
    WHERE l.id IN ({placeholders})
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=id_batch)
        print(f"Fetched {len(df)} rows of data.")
        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching listings data:", error)
        return None


# def fetch_listings_data(conn, id_batch, listings_table, embeddings_table):
#     """Fetch listings data using SQLAlchemy"""
#     engine = create_engine(
#         f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
#         f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}',
#         connect_args={'sslmode': os.getenv('DB_SSLMODE', 'require')}
#     )
    
#     query = f"""
#         SELECT l.*, le.front_image_embeddings
#         FROM {listings_table} l
#         JOIN {embeddings_table} le ON l.id = le.listings_id
#         WHERE l.id = ANY(%s)
#     """
    
#     return pd.read_sql_query(query, engine, params=[id_batch])

if __name__ == "__main__":
    conn = connect_to_postgres()
    if conn:
        total_count = count_pg_listings(conn)
        print(f"Total number of listings in PostgreSQL: {total_count}")

        conn.close()