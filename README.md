# Order Data Pipeline

## Overview
ETL pipeline that processes Zuora order data from CSV, with prepared Stripe API integration. Produces analytics-ready output.

## Approach
Python ETL script that:
1. Extracts and cleans Zuora order data from CSV
2. Prepares for Stripe API integration (disabled in code)
3. Outputs validated data to CSV for SQL analytics
4. Includes error handling and data quality checks

## Assumptions
- Input CSV contains: `order_id`, `customer_email`, `order_total`, `order_date`
- Dates are in ISO 8601 format (e.g., `2025-04-12T15:36:08.070203`)
- Missing `order_id` values are intentional (set to `-1`)
- Stripe API endpoint is unavailable (commented out in code)

## How to Run
```bash
# Install dependencies
pipenv install
pipenv shell

# Place orders_source_zuora.csv file in the project root

# Run pipeline (processes orders_source_zuora.csv)
pipenv run python main.py

# Output: combined_orders.csv
