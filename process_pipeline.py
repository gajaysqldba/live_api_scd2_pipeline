import sys
from google.cloud import bigquery

# Project Configuration
PROJECT_ID = "bqdemo-496217"  
bq_client = bigquery.Client(project=PROJECT_ID)

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
    load_job.result()  # Waits for the job to complete
    print(f"✅ Staging table successfully populated.")

def run_scd_type2_merge():
    """Executes the SCD Type 2 sequencing wrapped in a safe atomic SQL transaction."""
    print("🧠 Orchestrating SCD Type 2 transaction compilation logic...")
    
    unified_transaction_query = f"""
    BEGIN TRANSACTION;

      -- Phase 1: Close out modified records (Trusting the data values via pixel-to-pixel check)
      UPDATE `{PROD_TABLE}` AS prod
      SET prod.end_date = TIMESTAMP(staging.row_last_updated),
          prod.is_current = FALSE
      FROM `{STAGING_TABLE}` AS staging
      WHERE prod.customer_id = staging.customer_id
        AND prod.is_current = TRUE
        AND (
          COALESCE(TRIM(prod.name), '') != COALESCE(TRIM(staging.name), '')
          OR COALESCE(TRIM(prod.email), '') != COALESCE(TRIM(staging.email), '')
          OR COALESCE(TRIM(prod.subscription_tier), '') != COALESCE(TRIM(staging.subscription_tier), '')
        );

      -- Phase 2: Insert New & Updated Profiles using Pixel-to-Pixel Set Theory
      INSERT INTO `{PROD_TABLE}` (customer_id, name, email, subscription_tier, row_last_updated, start_date, end_date, is_current)
      SELECT 
          customer_id, name, email, subscription_tier,
          TIMESTAMP(row_last_updated), TIMESTAMP(row_last_updated),
          TIMESTAMP('9999-12-31 23:59:59'), TRUE
      FROM `{STAGING_TABLE}`

      EXCEPT DISTINCT

      SELECT 
          customer_id, name, email, subscription_tier,
          TIMESTAMP(row_last_updated), TIMESTAMP(row_last_updated),
          TIMESTAMP('9999-12-31 23:59:59'), TRUE
      FROM `{PROD_TABLE}`;

    COMMIT;
    """
    
    print("👉 Sending unified transaction block to BigQuery engine...")
    query_job = bq_client.query(unified_transaction_query)
    query_job.result()  # Waits for the transaction block to execute
    print("🚀 SCD Type 2 process complete. Production table updated successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Error: Missing filename argument.")
        print("💡 Usage: python scd2_pipeline.py customers_batch_20260606.csv")
        sys.exit(1)
        
    target_file = sys.argv[1]
    load_csv_to_staging(target_file)
    run_scd_type2_merge()