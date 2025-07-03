import requests
from datetime import datetime, timedelta
import datetime as dt

from snowflake.snowpark.functions import date_trunc, current_date, to_date
from snowflake.snowpark.functions import col, lit, when
from snowflake.snowpark.types import DateType
import snowflake.snowpark.functions as f


def get_orders_for_date(date, ACCESS_TOKEN, SHOP_ENDPOINT, QUERY_STRING):

    headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
    }

#SHOP_ENDPOINT = "https://shopmbg.myshopify.com/admin/api/2024-04/graphql.json"
    all_orders = []
    cursor = None

    start = datetime.strptime(date, "%Y-%m-%d")
    end = start + timedelta(days=1)
    #date_query_string = f"processed_at:>={start.strftime('%Y-%m-%dT00:00:00Z')} AND processed_at:<{end.strftime('%Y-%m-%dT00:00:00Z')}"

    date_query_string  = f"processed_at:={date}"

    while True:
        variables = {
            "cursor": cursor,
            "date": date_query_string
        }
        payload = {
            "query": QUERY_STRING,
            "variables": variables
        }

        # dbug print(payload)
        response = requests.post(SHOP_ENDPOINT, json=payload, headers=headers)
        response.raise_for_status()  # raise if bad response
        data = response.json()

        # Defensive: check for errors
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        orders_data = data["data"]["orders"]
        edges = orders_data["edges"]

        # Append current page orders
        all_orders.extend(edges)

        page_info = orders_data["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

    print(f"Fetched {len(all_orders)} orders for {date}")
   

    return all_orders  



def get_min_date_from_table(SELLING_PLAN_TABLE, DATE_COLUMN, SESSION) :

    try:
        min_date = SESSION.table(SELLING_PLAN_TABLE)\
            .select(to_date(col(DATE_COLUMN)).alias("PROCESSED_DATE")).distinct()\
            .agg(f.min(col("PROCESSED_DATE"))).collect()[0][0]
        
    except Exception :
        min_date = dt.now() - timedelta(days=1)

    return min_date

def get_max_datetime_from_table(SELLING_PLAN_TABLE, DATE_COLUMN, SESSION) :


    max_datetime = SESSION.table(SELLING_PLAN_TABLE)\
            .select((col(DATE_COLUMN)).alias("TIMESTAMP")).distinct()\
            .agg(f.max(col("TIMESTAMP"))).collect()[0][0]

    return max_datetime


def get_max_date_from_table(SELLING_PLAN_TABLE, DATE_COLUMN, SESSION) :

    try:
        max_date = SESSION.table(SELLING_PLAN_TABLE)\
            .select(to_date(col(DATE_COLUMN)).alias("PROCESSED_DATE")).distinct()\
            .agg(f.max(col("PROCESSED_DATE"))).collect()[0][0]
        
    except Exception :
        max_date = dt.now() - timedelta(days=1)

    return max_date


def backfill_date_generator(start_date=dt.date(2020, 1, 1), stop_date=dt.date(2020, 1, 1)):
    """
        generate dated up unit stop date
    
    """  
    current_date = start_date - timedelta(days=1)

    if current_date < stop_date:
        raise ValueError("Start date is before stop date")

    while current_date >= stop_date:
        yield current_date
        current_date -= timedelta(days=1)

    print(f"Reached stop date: {stop_date}. Generator exhausted.")


def get_orders_by_ids(order_ids, ACCESS_TOKEN, SHOP_ENDPOINT, QUERY_STRING):
    """
    Fetch orders from Shopify GraphQL API by a list of order IDs
    
    Args:
        order_ids (list): List of Shopify order IDs to fetch
        ACCESS_TOKEN (str): Shopify access token
        SHOP_ENDPOINT (str): Shopify GraphQL endpoint
        QUERY_STRING (str): GraphQL query string
        
    Returns:
        list: List of order objects
    """
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": ACCESS_TOKEN
    }

    all_orders = []
    cursor = None

    # Convert order IDs to a list of strings if they aren't already
    order_ids_query = format_order_numbers_query(order_ids)
    
    while True:
        variables = {
            "cursor": cursor,
            "queryString": order_ids_query
        }
        payload = {
            "query": QUERY_STRING,
            "variables": variables
        }

        response = requests.post(SHOP_ENDPOINT, json=payload, headers=headers)
        response.raise_for_status()  # raise if bad response
        data = response.json()

        # Defensive: check for errors
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        orders_data = data["data"]["orders"]
        edges = orders_data["edges"]

        # Append current page orders
        all_orders.extend(edges)

        page_info = orders_data["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

    print(f"Fetched {len(all_orders)} orders for {len(order_ids)} requested IDs")
    
    return all_orders


def format_order_numbers_query(order_numbers):
    """
    Convert a list of integer order numbers into a Shopify query string format.
    
    Args:
        order_numbers (list): List of integer order numbers
        
    Returns:
        str: Formatted query string in the format "name:#123 OR name:#456"
        
    Example:
        >>> format_order_numbers_query([732530, 732894])
        "name:#732530 OR name:#732894"
    """
    # Convert integers to strings and add the '#' prefix
    formatted_numbers = [f"name:#{num}" for num in order_numbers]
    
    # Join with ' OR '
    return " OR ".join(formatted_numbers)



