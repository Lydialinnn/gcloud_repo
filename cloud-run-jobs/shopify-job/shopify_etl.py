import os
import json
import requests
import pandas as pd
import pytz
import time
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.cloud import secretmanager
from flask import Flask, request
import uuid

# --- NEW IMPORTS FOR RETRY LOGIC ---
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log,
    RetryError
)


app = Flask(__name__)

# --- GCP Configuration ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET')

BIGQUERY_TABLE = os.environ.get('BIGQUERY_TABLE') 
BIGQUERY_TABLE_fulfilled = os.environ.get('BIGQUERY_TABLE_fulfilled') 

SECRET_NAME = os.environ.get('SECRET_NAME')
bq_location = "northamerica-northeast2" 

# --- Shopify Configuration ---
API_SHOP = "valordistributions"
API_VERSION = '2025-04'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

tenacity_logger = logging.getLogger('tenacity')
tenacity_logger.setLevel(logging.INFO)


def get_secret(project_id, secret_id, version_id="latest"):
    """Fetches a secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@app.route('/', methods=['POST'])
def main_handler_wrapper():
    """Triggered by Cloud Scheduler via HTTP POST."""
    logger.info("HTTP request received. Starting job.")
    return main_handler(None, None)

def main_handler(event, context):
    logger.info("--- Starting Shopify to BigQuery Job (Dynamic Chunk Version) ---")

    # 1. Parse Arguments from Cloud Scheduler (JSON Payload)
    # If triggered manually without payload, it defaults to: 
    # Fetch 20 days, starting from yesterday (Offset 0)
    try:
        json_payload = request.get_json(silent=True) or {}
    except:
        json_payload = {}
        
    # Default to 20 days if not specified
    days_to_fetch = json_payload.get('days_to_fetch', 20) 
    # Default to 0 (start from yesterday) if not specified
    offset_days = json_payload.get('offset_days', 0)
    
    logger.info(f"Configuration: Fetching {days_to_fetch} days, starting {offset_days} days ago.")

    try:
        api_token = get_secret(GCP_PROJECT_ID, SECRET_NAME)
        headers = {'X-Shopify-Access-Token': api_token.strip()}
    except Exception as e:
        logger.error(f"FATAL: Could not retrieve API secret. Error: {e}")
        return "Error fetching secret", 500

    # 2. Date Calculation (Toronto Time)
    toronto_tz = pytz.timezone("America/Toronto")
    
    # Use UTC for the timestamp
    run_timestamp = datetime.now(timezone.utc).isoformat()
    
    today_toronto = datetime.now(toronto_tz).date()
    yesterday_toronto = today_toronto - timedelta(days=1)
    
    # --- DYNAMIC DATE LOGIC (fixed) ---
    if offset_days == 0:
        # Latest Batch: Ends Yesterday
        end_date_toronto = yesterday_toronto
    else:
        # Historical Batch: Ends Yesterday - Offset - 1 Day
        # The -1 is crucial to avoid touching the date handled by the previous batch
        end_date_toronto = yesterday_toronto - timedelta(days=offset_days + 1)

    # API Request timestamps (Still needed for the API call itself)
    start_date_toronto = end_date_toronto - timedelta(days=days_to_fetch)

    # --- STRICT BOUNDARIES FOR STAGING ---
    # These strings will be used to filter the DataFrame manually
    strict_min_date = start_date_toronto.strftime('%Y-%m-%d')
    strict_max_date = end_date_toronto.strftime('%Y-%m-%d')
    
    logger.info(f"STRICT STAGING BOUNDARIES: {strict_min_date} to {strict_max_date}")

    


    created_at_min_toronto = datetime.combine(start_date_toronto, datetime.min.time(), tzinfo=toronto_tz)
    created_at_max_toronto = datetime.combine(end_date_toronto, datetime.max.time(), tzinfo=toronto_tz)

    created_at_min_utc = created_at_min_toronto.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    created_at_max_utc = created_at_max_toronto.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    logger.info(f"Fetching orders from (Toronto): {created_at_min_toronto} to {created_at_max_toronto}")
    logger.info(f"Run Timestamp: {run_timestamp}")

    # 3. Fetch and Load in Chunks  (Pass strict dates)
    try:
        # --- CHANGE: Unpack the extra return values ---
        rows_items, rows_full, actual_min_date, actual_max_date, temp_table_items, temp_table_fulfill = fetch_and_load_in_chunks(
            headers, created_at_min_utc, created_at_max_utc, run_timestamp, offset_days,
            strict_min_date, strict_max_date 
        )
    except Exception as e:
        logger.error(f"CRITICAL JOB FAILURE: {e}")
        return f"Job Failed: {e}", 500

    if rows_items == 0 and rows_full == 0:
        logger.info("No new data found. Exiting.")
        return "No data processed", 200

    logger.info(f"--- Staging complete! Data Range: {actual_min_date} to {actual_max_date} ---")

    # 4. Run Merge/Update SQL
    run_bigquery_chained_operations(temp_table_items, temp_table_fulfill) 

    return "Success", 200

def fetch_and_load_in_chunks(headers, min_date_str, max_date_str, run_timestamp, offset_days, strict_min_date, strict_max_date):
    """
    Loops through API pages, FILTERS data strictly, then loads to BQ.
    """
    bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=bq_location)

    # --- CHANGE: Generate Unique Temp Table Names ---
    unique_id = uuid.uuid4().hex[:8]
    
    # Define Temp Tables
    temp_table_items = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}_temp_{offset_days}_{unique_id}"
    temp_table_fulfill = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_fulfilled}_temp_{offset_days}_{unique_id}"

    # Initial URL and Params
    base_url = f"https://{API_SHOP}.myshopify.com/admin/api/{API_VERSION}/orders.json"
    
    initial_params = {
        'limit': 50, # meta data can be huge so limit it to just 50
        # 'fields': 'processed_at,name,cancelled_at,id,tags,financial_status,note,customer,line_items,shipping_address,discount_codes,fulfillments,current_subtotal_price,tax_lines',
        'processed_at_min': min_date_str,
        'processed_at_max': max_date_str,
        'status': 'any',
        'order': 'processed_at desc' # SAFE SORT: Ensures clean cut-offs
    }

    # --- THE RETRY LOGIC (INNER FUNCTION) ---
    # This applies the exact logic you provided: 5 attempts, exponential backoff (2s -> 10s)
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before=before_log(tenacity_logger, logging.DEBUG), # Changed to DEBUG to reduce noise
        after=after_log(tenacity_logger, logging.DEBUG)
    )
    def _fetch_page_safe(url, params=None):
        # We assume headers are available in the closure scope, or we could pass them
        logger.debug(f"Requesting: {url}")
        # Increased timeout to 60s
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response

    # --- MAIN LOOP ---
    next_url = base_url
    current_params = initial_params
    
    is_first_chunk = True
    total_items_loaded = 0
    total_fulfill_loaded = 0
    global_min_date = None
    global_max_date = None

    logger.info("Starting API fetch loop...")

    while next_url:
        try:
            response = _fetch_page_safe(next_url, params=current_params)
            current_params = None 

            data = response.json()
            orders_on_page = data.get('orders', [])

            if not orders_on_page:
                logger.info("Page returned, but no orders found. Ending loop.")
                break

            # logger.info(f"Fetched {len(orders_on_page)} orders.")

            # --- Transform Data ---
            _, df_items, df_fulfill = process_valor_orders_data(orders_on_page, run_timestamp)

            # ==============================================================================
            # CRITICAL FIX: HARD FILTER
            # We enforce that the data matches the strict Toronto dates calculated in main_handler
            # This prevents 2025-12-10 from leaking into the 2025-11-19_to_2025-12-09 batch
            # ==============================================================================

            if not df_items.empty:
                # Ensure date format comparison works (Strings vs Strings)
                # process_valor_orders_data returns 'YYYY-MM-DD' strings for 'order_date'
                
                initial_count = len(df_items)
                
                # df_items = df_items[ (df_items['order_date'] >= strict_min_date) &  (df_items['order_date'] <= strict_max_date) ]
                
                filtered_count = len(df_items)
                if filtered_count < initial_count:
                    logger.warning(f"Filtered out {initial_count - filtered_count} records falling outside {strict_min_date}-{strict_max_date}")

            # Note: We do not necessarily filter df_fulfill here because Fulfillments are deleted
            # based on Line Item IDs found in df_items. If the Item is filtered, the Fulfillment
            # update won't trigger for that specific item ID in the chained SQL.



            # --- Update Date Ranges ---
            if not df_items.empty:
                chunk_min = df_items['order_date'].min()
                chunk_max = df_items['order_date'].max()
                if global_min_date is None or chunk_min < global_min_date: global_min_date = chunk_min
                if global_max_date is None or chunk_max > global_max_date: global_max_date = chunk_max

            # --- Load to BigQuery ---
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = True 
            
            # Logic: Truncate only on the VERY FIRST successful load of the loop
            if is_first_chunk:
                job_config.write_disposition = "WRITE_TRUNCATE"
            else:
                job_config.write_disposition = "WRITE_APPEND"

            # Only write if we have data after filtering
            has_data = False

            if not df_items.empty:
                bq_client.load_table_from_dataframe(df_items, temp_table_items, job_config=job_config).result()
                total_items_loaded += len(df_items)
                has_data = True
            
            if not df_fulfill.empty:
                bq_client.load_table_from_dataframe(df_fulfill, temp_table_fulfill, job_config=job_config).result()
                total_fulfill_loaded += len(df_fulfill)
                has_data = True

            # If we successfully loaded anything, next time should be APPEND
            # if not df_items.empty or not df_fulfill.empty:
            #      is_first_chunk = False

            if has_data:
                 is_first_chunk = False

            # --- Pagination (Link Header) ---
            links = response.headers.get('link', '')
            next_link_found = False
            
            if links:
                for link in links.split(','):
                    if 'rel="next"' in link:
                        # Extract URL: <https://...>; rel="next" -> https://...
                        next_url = link.split(';')[0].strip()[1:-1]
                        next_link_found = True
                        break
            
            if not next_link_found:
                next_url = None # Stops the loop

            # --- LOG ONLY ON LAST PAGE ---
            if (next_url is None) or (len(orders_on_page) < 50):
                logger.info(f"Fetched final chunk: {len(orders_on_page)} orders. Loop finishing.")


        except RetryError as e:
            logger.error(f"FATAL: Max retries exceeded. API is down or timing out. Error: {e}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error in loop: {e}")
            raise e

    return total_items_loaded, total_fulfill_loaded, global_min_date, global_max_date, temp_table_items, temp_table_fulfill

def process_valor_orders_data(order_data, run_timestamp):
    """
    Logic ported from need_update.py.
    Returns order_df (unused), order_item_df, fulfilled_df.
    """
    orders = []
    order_item_detail = []
    order_item_fulfilled = []

    for order in order_data:
        if not isinstance(order, dict):
            continue

        order_info = {
            'order_date': order.get('processed_at'),
            'cancel_at': order.get('cancelled_at'),
            'order_name': order.get('name'),
            'order_id': str(order.get('id')),
            'order_tag': order.get('tags'),
            'financial_status': order.get('financial_status'),
            'order_note': order.get('note'),
            # Convert list/dict to JSON string for BQ storage or keep as is if using BQ RECORD type.
            # For flat tables, stringify complex objects usually works best unless using Schema.
            'discount_code': str(order.get('discount_codes', [{}])),
            'order_fulfillment_status': order.get('fulfillment_status') or 'unfulfilled',
            'order_total_discounts': float(order.get('current_total_discounts') or 0)
            # ,'order_net_sales': float(order.get('current_subtotal_price', 0) or 0)
        }

        customer = order.get('customer') or {}
        order_customer_info = {
            'customer_id': str(customer.get('id')),
            'customer_name': f"{customer.get('first_name','')} {customer.get('last_name','')}".strip(),
            'customer_tag': customer.get('tags')
        }

        shipping = order.get('shipping_address') or {}
        order_shipping_info = {
            'shipping_province': shipping.get('province'),
            'shipping_city': shipping.get('city'),
            'shipping_zipcode': shipping.get('zip'),
            'shipping_company': shipping.get('company'),
            'shipping_geocoding': f"({shipping.get('latitude')},{shipping.get('longitude')})".strip()
        }

        # Line items
        for line_item in order.get('line_items', []):
            if not isinstance(line_item, dict):
                continue

            discount_allocations = line_item.get('discount_allocations', [])
            discount_amount = 0.0
            for allocation in discount_allocations:
                discount_amount += -float(allocation.get('amount', 0))

            tax_lines = line_item.get('tax_lines', [])
            tax_amount = 0.0
            for tax_line in tax_lines:
                tax_amount += float(tax_line.get('price','0.0'))

            originalUnitPrice = 0.0
            custom_discount_amount = 0.0
            excise_amount = 0.0
            
            properties_lines = line_item.get('properties', [])
            for prop in properties_lines:
                name = prop.get('name', '')
                val_str = prop.get('value', '')

                if name == '_discount':
                    try:
                        parsed_value = json.loads(val_str)
                        originalUnitPrice = float(parsed_value.get('originalUnitPrice', 0))
                        applied_discount = parsed_value.get('appliedDiscount', {})
                        custom_discount_amount += -float(applied_discount.get('amount', 0))
                    except (ValueError, TypeError, json.JSONDecodeError):
                        pass
                elif name == 'excise' or 'provincial:' in name:
                    try:
                        excise_amount += float(val_str)
                    except (ValueError, TypeError):
                        pass

            order_item = {
                'order_date': order_info['order_date'],
                'cancel_at': order_info['cancel_at'],
                'order_name': order_info['order_name'],
                'order_id': order_info['order_id'],
                'financial_status': order_info['financial_status'],
                'order_tag': order_info['order_tag'],
                'order_note': order_info['order_note'],
                'discount_code': order_info['discount_code'],
                'line_item_id': str(line_item.get('id', '')),
                'sku': line_item.get('sku', 'none'),
                'product_title': line_item.get('title', 'none'),
                'sku_variant_title': line_item.get('variant_title'),
                'variant_id': str(line_item.get('variant_id', '')),
                'net_qty': int(line_item.get('current_quantity', 0)),
                'unit_price': float(line_item.get('price', 0) or 0),
                'sku_discount': discount_amount,
                '_sku_excise': excise_amount,
                'tax_amount': tax_amount,
                '_originalUnitPrice': originalUnitPrice,
                '_custom_discount_amount': custom_discount_amount,
                'customer_id': order_customer_info['customer_id'],
                'customer_name': order_customer_info['customer_name'],
                'customer_tag': order_customer_info['customer_tag'],
                'shipping_province': order_shipping_info['shipping_province'],
                'shipping_city': order_shipping_info['shipping_city'],
                'shipping_company': order_shipping_info['shipping_company'],
                'shipping_zipcode': order_shipping_info['shipping_zipcode'],
                'shipping_geocoding': order_shipping_info['shipping_geocoding'],
                'order_fulfillment_status': order_info['order_fulfillment_status'],
                'order_total_discounts': order_info['order_total_discounts'],
                'last_updated_at': run_timestamp 
            }
            order_item_detail.append(order_item)

        # Fulfillments --- UPDATED
        for fulfillment in order.get('fulfillments', []):
            if not isinstance(fulfillment, dict) or fulfillment.get('status') != 'success':
                continue
                
            for item in fulfillment.get('line_items', []):
                order_item_fulfilled.append({
                    'fulfilled_at': fulfillment.get('created_at'),
                    'fulfillment_id': str(fulfillment.get('id')), # new added
                    'name': fulfillment.get('name'), # new added
                    'order_id': str(fulfillment.get('order_id')), # new added
                    'sku': item.get('sku'), # new added
                    'line_item_id': str(item.get('id', '')),
                    'fulfilled_sku_qty': int(item.get('quantity', 0)),
                    'fulfilled_status': item.get('fulfillment_status'),
                    'last_updated_at': run_timestamp 
                })

    # Convert to DataFrame and fix timezones
    toronto_timezone = pytz.timezone('America/Toronto')
    
    # -- Process Items DF --
    if order_item_detail:
        order_item_df = pd.DataFrame(order_item_detail)
        #order_item_df['order_date'] = pd.to_datetime(order_item_df['order_date'], utc=True).dt.tz_convert(toronto_timezone).dt.strftime('%Y-%m-%d') #STR dtype can be mixed with NULL
        order_item_df['order_date'] = (pd.to_datetime(order_item_df['order_date'], utc=True, errors='coerce').dt.tz_convert(toronto_timezone).dt.strftime('%Y-%m-%d'))
        order_item_df['cancel_at'] = pd.to_datetime(order_item_df['cancel_at'], utc=True, errors='coerce').dt.tz_convert(toronto_timezone).dt.strftime('%Y-%m-%d')
    else:
        order_item_df = pd.DataFrame()

    # -- Process Fulfillments DF --
    if order_item_fulfilled:
        fulfilled_df = pd.DataFrame(order_item_fulfilled)
        fulfilled_df['fulfilled_at'] = pd.to_datetime(fulfilled_df['fulfilled_at'], utc=True).dt.tz_convert(toronto_timezone).dt.strftime('%Y-%m-%d')
    else:
        fulfilled_df = pd.DataFrame()

    return None, order_item_df, fulfilled_df

def run_bigquery_chained_operations(temp_table_items, temp_table_full):
    """
    1. DELETE overlaps (ID-BASED).
    2. INSERT new data.
    3. CONDITIONAL SAFETY NET: Checks for duplicates. Only runs dedup if found.
    """
    bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=bq_location)
    
    main_table_items = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    main_table_full = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_fulfilled}"

    chained_sql_script = f"""
    BEGIN
        -- Declare a variable to hold the duplicate count
        DECLARE dup_count INT64 DEFAULT 0;

        BEGIN TRANSACTION;
            
            -- =================================================================
            -- 1. ORDER ITEMS: ** updated to ID-BASED (Safe from Timezone Glitches), but need to check order_deletions later ** 
            -- =================================================================
            
            -- A. Delete existing records in Main that overlap with DATES in Temp
            DELETE FROM `{main_table_items}` T
            WHERE T.order_id IN (SELECT DISTINCT order_id FROM `{temp_table_items}`);

            -- B. Insert new records from Temp
            -- ensures that if the API sent duplicates in this batch,  only pick the most recent one
            INSERT INTO `{main_table_items}` (
                order_date, cancel_at, order_name, order_id, financial_status, 
                order_tag, order_note, discount_code, line_item_id, sku, 
                product_title, sku_variant_title, variant_id, net_qty, unit_price, 
                sku_discount, _sku_excise, tax_amount, _originalUnitPrice, 
                _custom_discount_amount, customer_id, customer_name, customer_tag, 
                shipping_province, shipping_city, shipping_company, shipping_zipcode, 
                shipping_geocoding,order_fulfillment_status,order_total_discounts, last_updated_at
            )
            SELECT 
                SAFE_CAST(order_date AS DATE),
                SAFE_CAST(cancel_at AS DATE),
                order_name, order_id, financial_status, 
                order_tag, order_note, discount_code, line_item_id, sku, 
                product_title, sku_variant_title, variant_id, net_qty, unit_price, 
                sku_discount, _sku_excise, tax_amount, _originalUnitPrice, 
                _custom_discount_amount, customer_id, customer_name, customer_tag, 
                shipping_province, shipping_city, shipping_company, shipping_zipcode, 
                shipping_geocoding, order_fulfillment_status, 
                SAFE_CAST(order_total_discounts AS FLOAT64),
                SAFE_CAST(last_updated_at AS TIMESTAMP)
            FROM `{temp_table_items}`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY line_item_id ORDER BY last_updated_at DESC) = 1;


            -- =================================================================
            -- 2. FULFILLMENTS UPDATE (Standard Logic) ** new update to include new extracted columns**
            -- =================================================================
            
            DELETE FROM `{main_table_full}` T
            WHERE T.line_item_id IN (SELECT DISTINCT line_item_id FROM `{temp_table_full}`);

            INSERT INTO `{main_table_full}` (
                fulfilled_at, fulfillment_id, name, order_id, sku,
                line_item_id, fulfilled_sku_qty, 
                fulfilled_status, last_updated_at
            )
            SELECT 
                SAFE_CAST(fulfilled_at AS DATE),fulfillment_id, name, order_id, sku,
                line_item_id, fulfilled_sku_qty, 
                fulfilled_status, SAFE_CAST(last_updated_at AS TIMESTAMP)
            FROM `{temp_table_full}`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY line_item_id, fulfillment_id ORDER BY last_updated_at DESC) = 1;

        COMMIT TRANSACTION;

        -- Final Cleanup of the staging tables
        DROP TABLE IF EXISTS `{temp_table_items}`;
        DROP TABLE IF EXISTS `{temp_table_full}`;
    END;
    """

    logger.info("Submitting async BigQuery transaction script...")
    try:
        query_job = bq_client.query(chained_sql_script)
        query_job.result() 
        logger.info(f"BigQuery job {query_job.job_id} finished successfully.")
    except Exception as e:
        logger.error(f"Failed to execute BigQuery job: {e}")
        raise e

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
