import sys
from google.cloud import bigquery

# Project Configuration
project_id = "bqdemo-496217"  
bq_client = bigquery.Client(project=project_id)

BUCKET_NAME = "gajay-customer-pipeline-data"
STAGING_TABLE = "bq_staging.customers_staging"
PROD_TABLE = "bq_production.customers_historical"

def load_csv_to_staging(batch_filename):
    """Wipes staging table and streams the new CSV file from GCS."""
    print(f"🔄 Starting ingestion sequence for file: {batch_filename}...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        autodetect=False,
    )
    
    gcs_uri = f"gs://{BUCKET_NAME}/landing_zone/{batch_filename}"
    load_job = bq_client.load_table_from_uri(gcs_uri, STAGING_TABLE, job_config=job_config)
    load_job.result()  
    print(f"✅ Staging table successfully populated.")

def run_scd_type2_merge():
    """Executes the SCD Type 2 sequencing wrapped in a safe atomic SQL transaction."""
    print("🧠 Orchestrating SCD Type 2 transaction compilation logic...")
    
    unified_transaction_query = f"""
    BEGIN TRANSACTION;
      -- Phase 1: Close out modified historical records (Checking name, email, and tier changes)
      UPDATE `{PROD_TABLE}` AS prod
      SET prod.end_date = TIMESTAMP(staging.row_last_updated),
          prod.is_current = FALSE
      FROM `{STAGING_TABLE}` AS staging
      WHERE prod.customer_id = staging.customer_id
        AND prod.is_current = TRUE
        AND (
          TRIM(prod.name) != TRIM(staging.name)
          OR TRIM(prod.email) != TRIM(staging.email) 
          OR TRIM(prod.subscription_tier) != TRIM(staging.subscription_tier)
        );

      -- Phase 2: Append brand-new rows and the fresh versions of updated profiles
      INSERT INTO `{PROD_TABLE}` (customer_id, name, email, subscription_tier, row_last_updated, start_date, end_date, is_current)
      SELECT 
          staging.customer_id, 
          staging.name, 
          staging.email, 
          staging.subscription_tier, 
          TIMESTAMP(staging.row_last_updated),
          TIMESTAMP(staging.row_last_updated),
          TIMESTAMP('9999-12-31 23:59:59'),
          TRUE
      FROM `{STAGING_TABLE}` AS staging
      LEFT JOIN `{PROD_TABLE}` AS prod
        ON staging.customer_id = prod.customer_id
       AND prod.is_current = TRUE
       AND TRIM(prod.name) = TRIM(staging.name)
       AND TRIM(prod.email) = TRIM(staging.email)
       AND TRIM(prod.subscription_tier) = TRIM(staging.subscription_tier)
      WHERE prod.customer_id IS NULL;
    COMMIT;
    """
    
    print("  👉 Sending unified transaction block to BigQuery engine...")
    query_job = bq_client.query(unified_transaction_query)
    query_job.result()  
    print("🚀 SCD Type 2 process complete. Production table updated successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Error: Missing filename argument.")
        sys.exit(1)
    target_file = sys.argv[1]
    load_csv_to_staging(target_file)
    run_scd_type2_merge()