from helpers.es import connect_to_elasticsearch, get_es_listings_ids, create_listings_index, index_listings_to_es
from helpers.db import connect_to_postgres, get_pg_listings_ids, fetch_listings_data
import time
import pandas as pd 
from dotenv import load_dotenv
import os

load_dotenv()

es_index_name = os.getenv('ES_INDEX_NAME')
listings_table = os.getenv("LISTINGS_TABLE")
embeddings_table = os.getenv("EMBEDDINGS_TABLE")

def compare_listings_ids(es_ids, pg_ids):
    """Compare listings IDs between Elasticsearch and PostgreSQL"""
    missing_in_es = pg_ids - es_ids
    missing_in_pg = es_ids - pg_ids
    return missing_in_es, missing_in_pg

def sync_listings():
    """Synchronize listings between PostgreSQL and Elasticsearch"""
    start_time = time.time()
    
    print("Starting synchronization process...")
    
    es_client = connect_to_elasticsearch()
    pg_conn = connect_to_postgres()

    if not es_client or not pg_conn:
        print("Failed to connect to Elasticsearch or PostgreSQL")
        return

    create_listings_index(es_client, es_index_name)

    print("Fetching Elasticsearch IDs...")
    es_start_time = time.time()
    es_ids = get_es_listings_ids(es_client, es_index_name)
    es_end_time = time.time()
    print(f"Retrieved {len(es_ids)} IDs from Elasticsearch in {es_end_time - es_start_time:.2f} seconds")

    print("\nFetching PostgreSQL IDs...")
    pg_start_time = time.time()
    pg_ids = set()
    for batch in get_pg_listings_ids(pg_conn):
        pg_ids.update(batch)
    pg_end_time = time.time()
    print(f"Retrieved {len(pg_ids)} IDs from PostgreSQL in {pg_end_time - pg_start_time:.2f} seconds")

    print("\nComparing IDs...")
    missing_in_es, missing_in_pg = compare_listings_ids(es_ids, pg_ids)

    print(f"IDs missing in Elasticsearch: {len(missing_in_es)}")
    print(f"IDs missing in PostgreSQL: {len(missing_in_pg)}")

    print("\nSample of IDs missing in Elasticsearch (up to 10):")
    print(list(missing_in_es)[:10])

    print("\nSample of IDs missing in PostgreSQL (up to 10):")
    print(list(missing_in_pg)[:10])

    print("\nID count summary:")
    print(f"Total IDs in Elasticsearch: {len(es_ids)}")
    print(f"Total IDs in PostgreSQL: {len(pg_ids)}")
    print(f"IDs in both: {len(es_ids.intersection(pg_ids))}")

    if missing_in_es:
        print(f"\nIndexing {len(missing_in_es)} missing documents to Elasticsearch...")
        batch_size = 500
        missing_list = list(missing_in_es)
        total_indexed = 0
        
        for i in range(0, len(missing_list), batch_size):
            batch_ids = missing_list[i:i+batch_size]
            df = fetch_listings_data(pg_conn, batch_ids, listings_table, embeddings_table)
            if df is not None and not df.empty:
                indexed = index_listings_to_es(es_client, df, es_index_name)
                total_indexed += indexed
                print(f"Indexed batch {i//batch_size + 1} ({indexed} documents)")
        
        print(f"\nTotal documents indexed to Elasticsearch: {total_indexed}")

    pg_conn.close()

    end_time = time.time()
    print(f"\nTotal synchronization process took {end_time - start_time:.2f} seconds")

    """
    # Process missing IDs in Elasticsearch
    if missing_in_es:
        print("Fetching data for IDs missing in Elasticsearch...")
        batch_size = 1000
        for i in range(0, len(missing_in_es), batch_size):
            batch_ids = list(missing_in_es)[i:i+batch_size]
            batch_df = get_joint_listings_batch(pg_conn, batch_ids)
            if batch_df is not None:
                # TODO: Index batch_df to Elasticsearch
                print(f"Indexed batch {i//batch_size + 1} to Elasticsearch")

    # TODO: Handle IDs missing in PostgreSQL (if needed)
    if missing_in_pg:
        print("IDs found in Elasticsearch but missing in PostgreSQL:")
        print(missing_in_pg)
    """

if __name__ == "__main__":
    sync_listings()