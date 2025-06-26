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
   - Copy `config/shopify_auth.yaml.template` to `config/shopify_auth.yaml`
   - Edit `config/shopify_auth.yaml` and add your Shopify API credentials
   - Set up Snowflake connection parameters
   
   ```bash
   cp config/shopify_auth.yaml.template config/shopify_auth.yaml
   # Edit config/shopify_auth.yaml with your credentials
   ```

   ⚠️ IMPORTANT: Never commit the `shopify_auth.yaml` file as it contains sensitive credentials.

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

## Security Notes

- The `config/shopify_auth.yaml` file contains sensitive credentials and is excluded from version control
- Always use the template file as a reference and create your local config file
- Never commit API tokens or sensitive credentials to the repository

## License

[MIT License](LICENSE)
