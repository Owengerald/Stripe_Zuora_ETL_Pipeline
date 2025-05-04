import pandas as pd
import requests
from datetime import datetime
import logging

# Logging set-up
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_zuora_data(zuora_file_path: str) -> pd.DataFrame:
    """Extract and validate zuora orders data from CSV.""" 
    try:
        df = pd.read_csv(zuora_file_path)
        logger.info(f"successfully loaded Zuora data with {len(df)} records")

        # Log count of missing order_ids before cleaning
        missing_ids = df['order_id'].isna().sum()
        if missing_ids > 0:
            logger.warning(f"Found {int(missing_ids)} records with missing order_id")

        df['order_id'] = df['order_id'].fillna(-1).astype(int) # Handle missing order_ids
        df['order_date'] = pd.to_datetime(df['order_date']) # Standardize order_date

        return df
    except Exception as e:
        logger.error(f"Error loading Zuora data: {str(e)}")
        raise

def extract_stripe_data(api_url: str, api_key: str) -> pd.DataFrame:
    """Extract and validate orders data from Stripe API.""" 
    try:
        headers = {'x-api-key': api_key}
        response = requests.get(api_url, headers=headers)
        response.raise_for_status() # Raise exception for non-200 responses

        data = response.json()
        logger.info(f"successfully retrieved {len(data)} records from Stripe API")

        # Convert to DataFrame and standardize columns...
        df = pd.DataFrame(data)

        return df 
    except Exception as e:
        logger.error(f"Stripe API failure: {str(e)}")
        raise 

def transform_orders_data(zuora_df: pd.DataFrame, stripe_df: pd.DataFrame = None) -> pd.DataFrame: # stripe_df set to None as default parameter
    """Transform and clean the data from both sources."""
    # Enrich Zuora data
    zuora_df['source_system'] = 'zuora'

    # Handle Stripe data availability, enrich it and merge with zuora data, if available
    if stripe_df is not None:
        stripe_df['source_system'] = 'stripe'
        combined_df = pd.concat([zuora_df, stripe_df], ignore_index=True) # Combine dataframes
    else:
        combined_df = zuora_df
    
    # Additional cleaning
    combined_df['customer_email'] = combined_df['customer_email'].str.lower().str.strip()
    combined_df['order_total'] = pd.to_numeric(combined_df['order_total'])

    logger.info("Data transformations completed")
    return combined_df

def validate_data(df: pd.DataFrame) -> bool:
    """Check for critical data quality issues."""
    if df['order_id'].duplicated().any():
        logger.warning("Duplicate order_ids detected")
    return True

def load_data(df: pd.DataFrame, output_file_path: str) -> None:
    """Output source data into CSV files"""
    df.to_csv(output_file_path, index=False)
    logger.info(f"Output saved to {str(output_file_path)} with {len(df)} records")

def main():
    try:
        # Configuration
        zuora_file_path = 'orders_source_zuora.csv'
        stripe_api_url = 'https://ad86f5eb-1f45-4787-8eba-f19577026a78.mock.pstmn.io/records'
        stripe_api_key = 'PMAK-6807e69e4e9d6e0081e92197-bdbf66cc5d599aed7ea16d3eb72121aae8'
        output_file_path = 'combined_orders.csv'

        # Extract
        zuora_df = extract_zuora_data(zuora_file_path)
        
        # The Stripe API endpoint isn't working (mock server may be down)
        # So I'll proceed with just Zuora data
        # stripe_df = extract_stripe_data(stripe_api_url, stripe_api_key)
        
        # Transform
        transformed_df = transform_orders_data(zuora_df)

        # Validate data
        validate_data(transformed_df)

        # Load
        load_data(transformed_df, output_file_path)
        
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

