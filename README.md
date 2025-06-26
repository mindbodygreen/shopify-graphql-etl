# Shopify GraphQL ETL

A Python-based ETL pipeline for extracting order data from Shopify using GraphQL API and loading it into Snowflake.

## Features

- Extracts order data from Shopify using GraphQL API
- Processes order details including line items and tags
- Loads data into Snowflake database
- Supports incremental data loading
- Handles pagination for large datasets

## Prerequisites

- Python 3.9+
- Snowflake account with appropriate permissions
- Shopify Admin API access
- Required Python packages (see requirements.txt)

## Setup

1. Clone the repository:
```bash
git clone https://github.com/mindbodygreen/shopify-graphql-etl.git
cd shopify-graphql-etl
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure authentication:
   - Create `config/shopify_auth.yaml` with your Shopify API credentials
   - Set up Snowflake connection parameters

## Usage

The main ETL process can be run through the Jupyter notebook:
```bash
jupyter notebook notebooks/SELLINGPLAN\ -\ SHOPIFY_API.ipynb
```

## Project Structure

- `notebooks/`: Jupyter notebooks for ETL processes
- `scripts/`: Python utility scripts
- `config/`: Configuration files
- `graphql/`: GraphQL query definitions
- `tests/`: Test files

## License

[MIT License](LICENSE)
