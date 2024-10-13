# LISTINGS-INDEX-UPDATER

This project synchronizes listing data between a PostgreSQL database and Elasticsearch, ensuring consistency between the two data stores.

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

   Note: If `requirements.txt` is missing, generate it using:
   ```
   pip freeze > requirements.txt
   ```

4. Ensure your `.env` file is properly configured with necessary environment variables.

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