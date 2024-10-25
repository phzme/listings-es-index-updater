# from helpers.es import connect_to_elasticsearch, get_es_listings_ids, create_listings_index, index_listings_to_es
from helpers.db import connect_to_postgres, get_pg_listings_ids, fetch_listings_data
from helpers.ts import (
    connect_to_typesense,
    get_typesense_listings_ids,
    create_listings_collection,
    index_listings_to_typesense
)
import time
import pandas as pd 
from dotenv import load_dotenv
import os

load_dotenv()

ts_collection_name  = os.getenv('TS_LISTING_INDEX_NAME')
listings_table = os.getenv("LISTINGS_TABLE")
embeddings_table = os.getenv("EMBEDDINGS_TABLE")

def compare_listings_ids(ts_ids, pg_ids):
    """Compare listings IDs between Elasticsearch and PostgreSQL"""
    missing_in_ts = pg_ids - ts_ids
    missing_in_pg = ts_ids - pg_ids
    return missing_in_ts, missing_in_pg

def sync_listings():
    """Synchronize listings between PostgreSQL and TypeSense"""
    start_time = time.time()
    
    print("Starting synchronization process...")
    
    # Connect to services
    try:
        ts_client = connect_to_typesense()
        pg_conn = connect_to_postgres()
    except Exception as e:
        print(f"Failed to connect to Typesense or PostgreSQL: {e}")
        return

    if not ts_collection_name:
        print("TS_LISTING_INDEX_NAME environment variable not set")
        return

    # Ensure collection exists
    create_listings_collection(ts_client, ts_collection_name)

    # Get Typesense IDs
    print("Fetching Typesense IDs...")
    ts_start_time = time.time()
    ts_ids = get_typesense_listings_ids(ts_client, ts_collection_name)
    ts_end_time = time.time()
    print(f"Retrieved {len(ts_ids)} IDs from Typesense in {ts_end_time - ts_start_time:.2f} seconds")

    # Get PostgreSQL IDs
    print("\nFetching PostgreSQL IDs...")
    pg_start_time = time.time()
    pg_ids = set()
    for batch in get_pg_listings_ids(pg_conn):
        pg_ids.update(batch)
    pg_end_time = time.time()
    print(f"Retrieved {len(pg_ids)} IDs from PostgreSQL in {pg_end_time - pg_start_time:.2f} seconds")

    # Compare IDs
    print("\nComparing IDs...")
    missing_in_ts, missing_in_pg = compare_listings_ids(ts_ids, pg_ids)

    print(f"IDs missing in Typesense: {len(missing_in_ts)}")
    print(f"IDs missing in PostgreSQL: {len(missing_in_pg)}")

    print("\nSample of IDs missing in Typesense (up to 10):")
    print(list(missing_in_ts)[:10])

    print("\nSample of IDs missing in PostgreSQL (up to 10):")
    print(list(missing_in_pg)[:10])

    print("\nID count summary:")
    print(f"Total IDs in Typesense: {len(ts_ids)}")
    print(f"Total IDs in PostgreSQL: {len(pg_ids)}")
    print(f"IDs in both: {len(ts_ids.intersection(pg_ids))}")

    # Handle missing documents
    if missing_in_ts:
        print(f"\nIndexing {len(missing_in_ts)} missing documents to Typesense...")
        batch_size = 500
        missing_list = list(missing_in_ts)
        total_indexed = 0
        
        for i in range(0, len(missing_list), batch_size):
            batch_ids = missing_list[i:i+batch_size]
            df = fetch_listings_data(pg_conn, batch_ids, listings_table, embeddings_table)
            
            if df is not None and not df.empty:
                indexed = index_listings_to_typesense(ts_client, df, ts_collection_name)
                total_indexed += indexed
                print(f"Indexed batch {i//batch_size + 1} ({indexed} documents)")
        
        print(f"\nTotal documents indexed to Typesense: {total_indexed}")

    # Handle documents that should be removed from Typesense
    if missing_in_pg:
        print(f"\nRemoving {len(missing_in_pg)} documents from Typesense that are no longer in PostgreSQL...")
        deleted_count = 0
        for doc_id in missing_in_pg:
            try:
                ts_client.collections[ts_collection_name].documents[doc_id].delete()
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting document {doc_id}: {e}")
        
        print(f"Removed {deleted_count} documents from Typesense")

    # Cleanup
    pg_conn.close()

    end_time = time.time()
    print(f"\nTotal synchronization process took {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    sync_listings()