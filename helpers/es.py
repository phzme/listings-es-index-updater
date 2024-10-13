import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
import numpy as np

# Load environment variables from .env file
load_dotenv()

# Elasticsearch connection details from environment variables
ES_URL = os.getenv('ES_URL')
ES_USER = os.getenv('ES_USER')
ES_PORT = int(os.getenv('ES_PORT'))
ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_PEM_PATH = os.getenv('ES_PEM_PATH')

def connect_to_elasticsearch():
    """Establish connection to Elasticsearch"""
    es = Elasticsearch(
        ['{}:{}'.format(ES_URL, ES_PORT)],
        use_ssl=True,
        verify_certs=True,
        ca_certs=ES_PEM_PATH,
        http_auth=(ES_USER, ES_PASSWORD)
    )
    if es.ping():
        print("Successfully connected to Elasticsearch!")
    else:
        print("Failed to connect to Elasticsearch.")
    return es

def create_listings_index(es_client, index_name):
    """Create the listings index with appropriate mapping"""
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "release_id": {"type": "keyword"},
                "meta_text": {"type": "text"},
                "barcode": {"type": "keyword"},
                "data_source": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "price": {"type": "text"},
                "currency": {"type": "text"},
                "front_image_embeddings": {
                    "type": "dense_vector",
                    "dims": 512  # Adjust this based on the actual dimensions of your embeddings
                }
            }
        }
    }

    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created with the specified mapping.")
    else:
        print(f"Index '{index_name}' already exists.")
        print("skipping..")


def get_es_listings_ids(es_client, index_name, batch_size=1000):
    """
    Get all listings IDs from Elasticsearch using the scroll API
    """
    query = {
        "query": {"match_all": {}},
        "_source": ["id"]
    }
    
    # Initialize the scroll
    result = es_client.search(index=index_name, body=query, scroll='2m', size=batch_size)
    scroll_id = result['_scroll_id']
    hits = result['hits']['hits']
    
    all_ids = set()

    while hits:
        # Process current batch
        batch_ids = {hit['_source']['id'] for hit in hits}
        all_ids.update(batch_ids)
        
        # Get the next batch
        result = es_client.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        hits = result['hits']['hits']

    # Clear the scroll to free up resources
    es_client.clear_scroll(scroll_id=scroll_id)

    print(f"Retrieved {len(all_ids)} IDs from Elasticsearch")
    return all_ids


def convert_df_types(df):
    """Convert DataFrame types to match Elasticsearch mapping"""
    df['id'] = df['id'].astype(str)
    df['release_id'] = df['release_id'].astype(str)
    df['meta_text'] = df['meta_text'].astype(str)
    df['barcode'] = df['barcode'].astype(str)
    df['data_source'] = df['data_source'].astype(str)
    df['source_id'] = df['source_id'].astype(str)
    df['price'] = df['price'].astype(str)
    df['currency'] = df['currency'].astype(str)
     
    return df


def index_listings_to_es(es_client, df, index_name):
    create_listings_index(es_client, index_name)
    df = convert_df_types(df)
    
    actions = ({
        "_index": index_name,
        "_id": row['id'],
        "_source": {k: v for k, v in row.items() if v is not None and v != []}
    } for _, row in df.iterrows())

    try:
        success, errors = helpers.bulk(es_client, actions, stats_only=False)
        print(f"Indexed {success} documents to Elasticsearch.")
        if errors:
            print(f"Encountered {len(errors)} errors during indexing.")
            for error in errors[:5]:
                print(f"Error: {error}")
        return success
    except Exception as e:
        print(f"Error indexing to Elasticsearch: {e}")
        return 0


if __name__ == "__main__":
    # This block will run if the script is executed directly
    es = connect_to_elasticsearch()
    create_listings_index(es, index_name="listings_es_index_v2")
    # You can add more test or setup code here