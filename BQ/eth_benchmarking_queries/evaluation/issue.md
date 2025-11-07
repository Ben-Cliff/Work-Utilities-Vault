# BigQuery Evaluation Script Fails with Access Denied Error

## Summary

The `run_eval_queries.sh` script is failing to execute BigQuery jobs due to a permission error when attempting to query a public dataset.

## Steps to Reproduce

1.  Modified the `run_eval_queries.sh` script to run BigQuery jobs in the `us-central1` location. The following line was changed in the `run_query` function:

    **Before:**
    ```bash
    job_submission_output=$(bq query --nosync --use_legacy_sql=false --format=json "$query" 2>&1)
    ```

    **After:**
    ```bash
    job_submission_output=$(bq query --nosync --use_legacy_sql=false --format=json --location=us-central1 "$query" 2>&1)
    ```
2.  Executed the script using `./run_eval_queries.sh`.

## Observed Behavior

The script fails, and the `run_eval_queries.log` file shows the following error:

```
BigQuery error in query operation: Error processing job 'tony-allen:bqjob_r23bd0f363e8a9cc0_0000019a5dd709dc_1': Access Denied: Table bigquery-public-data:crypto_ethereum_classic.transactions: User does not have permission to query table bigquery-public-data:crypto_ethereum_classic.transactions, or perhaps it does not exist.
```

## Root Cause Analysis

The error message "Access Denied" indicates that the user account running the script does not have the required IAM permissions to query the public dataset `bigquery-public-data.crypto_ethereum_classic.transactions`.

This is not an issue with the script's logic or the query itself, but rather a configuration issue within the Google Cloud environment. The most likely cause is a **VPC Service Controls** policy that prevents resources within the project from accessing data outside of a defined security perimeter.

## Recommended Next Steps

To resolve this issue, the user needs to contact their Google Cloud administrator and request the following:

1.  **Investigate VPC Service Controls**: Check if there is a VPC Service Controls policy in place that is blocking access to BigQuery public datasets.
2.  **Adjust Permissions**: If a policy is in place, the administrator will need to adjust it to allow access to the `bigquery-public-data` project, or specifically to the `crypto_ethereum_classic` dataset.
3.  **Verify IAM Roles**: Ensure the user account has the necessary BigQuery IAM roles (e.g., `BigQuery User`) to run queries.
