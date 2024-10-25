import os
import time
from dotenv import load_dotenv
import pandas as pd
from typesense import Client
import numpy as np
from typing import List, Set, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables from .env file
load_dotenv()

# TypeSense connection details from environment variables
TS_LISTING_INDEX_NAME= os.getenv('TS_LISTING_INDEX_NAME')
TS_HOST= os.getenv('TS_HOST')
TS_PORT= os.getenv('TS_PORT')
TS_PROTOCOL= os.getenv('TS_PROTOCOL')
TS_API_KEY= os.getenv('TS_API_KEY')


def connect_to_typesense() -> Client:
    """Establish connection to Typesense"""
    client = Client({
        'nodes': [{
            'host': TS_HOST,
            'port': TS_PORT,
            'protocol': TS_PROTOCOL
        }],
        'api_key': TS_API_KEY,
        'connection_timeout_seconds': 10,  # Increased from 2
        'retry_interval_seconds': 0.1,
        'num_retries': 3,
        'socket_timeout_seconds': 30  # Added socket timeout
    })
    
    try:
        # Test connection by retrieving collections list
        client.collections.retrieve()
        print("Successfully connected to Typesense!")
    except Exception as e:
        print(f"Failed to connect to Typesense: {e}")
        raise
    
    return client

def create_listings_collection(client: Client, collection_name: str):
    """Create the listings collection with appropriate schema"""
    schema = {
        'name': collection_name,
        'fields': [
            {'name': 'id', 'type': 'string', 'sort': True},
            {'name': 'release_id', 'type': 'string'},
            {'name': 'meta_text', 'type': 'string'},
            {'name': 'barcode', 'type': 'string'},
            {'name': 'data_source', 'type': 'string'},
            {'name': 'source_id', 'type': 'string'},
            {'name': 'price', 'type': 'float', 'optional': True},
            {'name': 'currency', 'type': 'string', 'optional': True},
            {'name': 'front_image_embeddings', 'type': 'float[]', 'dim': 512}
        ],
        # 'default_sorting_field': 'id'
    }

    try:
        client.collections.create(schema)
        print(f"Collection '{collection_name}' created with the specified schema.")
    except Exception as e:
        if 'already exists' in str(e):
            print(f"Collection '{collection_name}' already exists.")
        else:
            raise
        
        
def get_typesense_listings_ids(client: Client, collection_name: str) -> Set[str]:
    """
    Get all listings IDs from Typesense
    """
    all_ids = set()
    batch_size = 250  # Typesense recommended batch size
    current_page = 1
    
    while True:
        search_parameters = {
            'q': '*',
            'per_page': batch_size,
            'page': current_page,
        }
        
        try:
            result = client.collections[collection_name].documents.search(search_parameters)
            hits = result['hits']
            
            if not hits:
                break
                
            batch_ids = {hit['document']['id'] for hit in hits}
            all_ids.update(batch_ids)
            current_page += 1
            
        except Exception as e:
            print(f"Error retrieving IDs from Typesense: {e}")
            break

    print(f"Retrieved {len(all_ids)} IDs from Typesense")
    return all_ids


def convert_df_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame types to match Typesense schema"""
    df['id'] = df['id'].astype(str)
    df['release_id'] = df['release_id'].astype(str)
    df['meta_text'] = df['meta_text'].astype(str)
    df['barcode'] = df['barcode'].astype(str)
    df['data_source'] = df['data_source'].astype(str)
    df['source_id'] = df['source_id'].astype(str)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')  # Convert to float, invalid values become NaN
    df['currency'] = df['currency'].astype(str)
    
    return df

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def index_batch_with_retry(client: Client, documents: List[dict], collection_name: str) -> int:
    """Index a batch of documents with retry logic"""
    try:
        results = client.collections[collection_name].documents.import_(documents)
        success_count = sum(1 for r in results if 'success' in r and r['success'])
        error_count = len(results) - success_count
        
        if error_count > 0:
            print(f"Encountered {error_count} errors in batch.")
            for error in [r for r in results if 'success' not in r or not r['success']][:5]:
                print(f"Error: {error}")
        
        return success_count
    except Exception as e:
        print(f"Error in batch: {e}")
        raise


def index_listings_to_typesense(client: Client, df: pd.DataFrame, collection_name: str) -> int:
    """
    Index listings data to Typesense
    Returns the number of successfully indexed documents
    """
    create_listings_collection(client, collection_name)
    df = convert_df_types(df)
    
    # Process in smaller batches
    batch_size = 100  # Reduced batch size
    total_success = 0
    total_documents = len(df)
    
    for start_idx in range(0, total_documents, batch_size):
        end_idx = min(start_idx + batch_size, total_documents)
        batch_df = df.iloc[start_idx:end_idx]
        
        documents = []
        for _, row in batch_df.iterrows():
            doc = {
                'id': row['id'],
                'release_id': row['release_id'],
                'meta_text': row['meta_text'],
                'barcode': row['barcode'],
                'data_source': row['data_source'],
                'source_id': row['source_id'],
                'price': float(row['price']) if pd.notnull(row['price']) else None,
                'currency': row['currency'] if pd.notnull(row['currency']) else None,
                'front_image_embeddings': (row['front_image_embeddings'].tolist() 
                                         if isinstance(row['front_image_embeddings'], np.ndarray)
                                         else row['front_image_embeddings'] 
                                         if isinstance(row['front_image_embeddings'], list) 
                                         else None)
            }
            documents.append({k: v for k, v in doc.items() if v is not None})

        try:
            success_count = index_batch_with_retry(client, documents, collection_name)
            total_success += success_count
            print(f"Progress: {end_idx}/{total_documents} documents processed. "
                  f"Batch success: {success_count}/{len(documents)}")
            
            # Add a small delay between batches
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Failed to index batch {start_idx//batch_size + 1}: {e}")
            continue

    print(f"\nTotal documents successfully indexed: {total_success}/{total_documents}")
    return total_success
    
if __name__ == "__main__":
    # This block will run if the script is executed directly
    ts = connect_to_typesense()
    create_listings_collection(ts, index_name="listings")
    # You can add more test or setup code here