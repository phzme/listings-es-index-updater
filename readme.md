# LISTINGS-INDEX-UPDATER

This project synchronizes listing data between a PostgreSQL database and Elasticsearch, ensuring consistency between the two data stores.

![Screenshot 2024-10-14 011401](https://github.com/user-attachments/assets/3ef9f184-2307-45a2-a4d8-01eec3210566)

## Project Structure

```
LISTINGS-INDEX-UPDATER/
│
├── helpers/
│   ├── db.py
│   └── es.py
│
├── venv/
│
├── .env
├── .pem
└── sync.py
```

## Files Description

- `helpers/db.py`: Handles PostgreSQL database connections and operations.
- `helpers/es.py`: Manages Elasticsearch connections and operations.
- `sync.py`: Orchestrates the synchronization process between PostgreSQL and Elasticsearch.
- `.env`: Contains environment variables for database and Elasticsearch configurations.
- `.pem`: SSL certificate for secure connections.

## Prerequisites

- Python 3.x
- PostgreSQL
- Elasticsearch
- Virtual environment (venv)

## Setup

1. Clone the repository:
   ```
   git clone <your-repo-url>
   cd LISTINGS-INDEX-UPDATER
   ```

2. Create and activate the virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Configure the `.env` file:
   Create a `.env` file in the project root with the following structure:

   ```
   # Elasticsearch configuration
   ES_INDEX_NAME=<your-elasticsearch-index-name>
   ES_URL=<your-elasticsearch-url>
   ES_USER=<your-elasticsearch-username>
   ES_PORT=<your-elasticsearch-port>
   ES_PASSWORD=<your-elasticsearch-password>
   ES_PEM_PATH=<path-to-your-pem-file>

   # PostgreSQL configuration
   DB_HOST=<your-postgresql-host>
   DB_NAME=<your-database-name>
   DB_USER=<your-database-username>
   DB_PASSWORD=<your-database-password>
   DB_PORT=<your-database-port>
   DB_SSLMODE=<your-ssl-mode>

   # Table names
   LISTINGS_TABLE=<your-listings-table-name>
   EMBEDDINGS_TABLE=<your-embeddings-table-name>
   ```

   Replace the placeholder values with your actual configuration details.

## Usage

To run the synchronization process:

```
python sync.py
```

This script will:
1. Connect to both PostgreSQL and Elasticsearch.
2. Fetch listing IDs from both sources.
3. Compare the IDs to find discrepancies.
4. Update Elasticsearch with any missing listings from PostgreSQL.
