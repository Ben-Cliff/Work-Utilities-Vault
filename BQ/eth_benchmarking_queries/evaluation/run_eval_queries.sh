#!/bin/bash

# This script runs a series of hardcoded BigQuery evaluation queries,
# logs verbose output, and creates a clean summary file with execution statistics.

LOG_FILE="run_eval_queries.log"
SUMMARY_FILE="execution_summary.tsv"

# Redirect all stdout and stderr to the log file
exec &> "$LOG_FILE"

# Exit immediately if a command exits with a non-zero status.
set -ea
# Print each command to stderr before executing it for debugging.
set -x

# Create summary file and write header
echo -e "Query_Name\tJob_ID\tDuration_ms\tBytes_Processed\tSlot_Milliseconds" > "$SUMMARY_FILE"

echo "Script started at $(date). Logging output to $LOG_FILE"
echo "Summary will be written to $SUMMARY_FILE"
echo "==========================================================="

# --- Prerequisite check ---
echo "Checking for prerequisites..."
if ! command -v bq &> /dev/null; then
    echo "[FATAL] 'bq' command not found. Please ensure the Google Cloud SDK is installed and in your PATH."
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "[FATAL] 'jq' command not found. Please install jq to parse JSON output."
    exit 1
fi
echo "Prerequisites 'bq' and 'jq' found."
echo "--------------------------------------------------"


# --- Query Definitions ---

QUERY_DAILY_AVG_UNOPTIMIZED=$(cat <<'EOF'
SELECT
    -- Calculating averages
    AVG(CAST(t.gas_price AS NUMERIC)) AS avg_gas_price_wei,
    AVG(t.value) AS avg_transaction_value_wei
FROM
    -- Scan the entire transactions table first
    `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
WHERE
    -- 1. Find the latest date by re-calculating the MAX date for every row,
    -- or by joining the max_date CTE without an explicit ON clause,
    -- leading to a non-sargable predicate against the entire table.
    DATE(t.block_timestamp) = (
        SELECT MAX(DATE(block_timestamp))
        FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
    )
EOF
)

QUERY_DAILY_AVG_OPTIMIZED=$(cat <<'EOF'
WITH max_date AS (
    -- 1. Determine the latest date once in an efficient, independent step.
    -- This operation is fast because it's a single aggregation over the table.
    SELECT MAX(DATE(block_timestamp)) AS max_dt
    FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
)
SELECT
    -- 2. Calculate the average gas price (casting INTEGER to NUMERIC for precision)
    AVG(CAST(t.gas_price AS NUMERIC)) AS avg_gas_price_wei,
    -- 3. Calculate the average transaction value (using the existing NUMERIC value column)
    AVG(t.value) AS avg_transaction_value_wei
FROM
    `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
INNER JOIN
    -- 4. Join the main table to the single-row CTE, ensuring the filter is pushed down
    -- and the table is only scanned for the single target date.
    max_date AS m ON DATE(t.block_timestamp) = m.max_dt
EOF
)

QUERY_ERC20_UNOPTIMIZED=$(cat <<'EOF'
WITH ERC20_Contracts AS (
    -- CTE 1: Filters the small 'contracts' dimension table once to identify ERC20s.
    SELECT address 
    FROM `bigquery-public-data.crypto_ethereum_classic.contracts` 
    WHERE is_erc20 = TRUE
),
MonthlyLogCounts AS (
    -- CTE 2: Filters early and aggregates the massive 'logs' table to find high-activity contracts (> 1,000 logs/month).
    SELECT
        t1.address,
        FORMAT_DATE('%Y-%m', t1.block_timestamp) AS transaction_month, 
        COUNT(t1.log_index) AS log_event_count 
    FROM
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t1
    INNER JOIN
        ERC20_Contracts AS t2 ON t1.address = t2.address
    GROUP BY
        1, 2
    HAVING
        log_event_count > 1000
),
FrequentTopics AS (
    -- CTE 3: Joins the already filtered, small result from CTE 2 back to the 'logs' table
    -- only for the relevant contracts/months to find the topic frequency (event signature).
    SELECT
        t1.address,
        t1.transaction_month,
        topic,
        -- Applies a rank over the small, relevant subset of data.
        ROW_NUMBER() OVER (PARTITION BY t1.address, t1.transaction_month ORDER BY COUNT(topic) DESC) AS topic_rank
    FROM
        MonthlyLogCounts AS t1
    INNER JOIN
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t2 
        ON t1.address = t2.address AND FORMAT_DATE('%Y-%m', t2.block_timestamp) = t1.transaction_month
    CROSS JOIN
        UNNEST(t2.topics) AS topic WITH OFFSET AS topic_index
    WHERE
        topic_index = 0 -- Isolates the event signature hash (topics[0])
    GROUP BY
        1, 2, 3
)
-- Final Query: Selects the result ranked 1 (the most frequent event hash) for each contract/month.
SELECT
    COUNT(DISTINCT address) AS unique_erc20_contracts_exceeding_1k,
    ARRAY_AGG(
        STRUCT(t1.address, t1.transaction_month, t1.topic AS most_frequent_signature_hash) 
        ORDER BY t1.address, t1.transaction_month
    ) AS details_by_contract_and_month
FROM FrequentTopics AS t1
WHERE t1.topic_rank = 1
EOF
)

QUERY_ERC20_OPTIMIZED=$(cat <<'EOF'
WITH ERC20_Contracts AS (
    SELECT address FROM `bigquery-public-data.crypto_ethereum_classic.contracts` WHERE is_erc20 = TRUE
),
MonthlyLogCounts AS (
    SELECT
        t1.address,
        FORMAT_DATE('%Y-%m', t1.block_timestamp) AS transaction_month, 
        COUNT(t1.log_index) AS log_event_count 
    FROM
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t1
    INNER JOIN
        ERC20_Contracts AS t2 ON t1.address = t2.address
    GROUP BY
        1, 2
    HAVING
        log_event_count > 1000
),
FrequentTopics AS (
    SELECT
        t1.address,
        t1.transaction_month,
        topic,
        ROW_NUMBER() OVER (PARTITION BY t1.address, t1.transaction_month ORDER BY COUNT(topic) DESC) AS topic_rank
    FROM
        MonthlyLogCounts AS t1 -- Joins the already filtered and aggregated small list
    INNER JOIN
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t2 
        ON t1.address = t2.address AND FORMAT_DATE('%Y-%m', t2.block_timestamp) = t1.transaction_month
    CROSS JOIN
        UNNEST(t2.topics) AS topic WITH OFFSET AS topic_index
    WHERE
        topic_index = 0
    GROUP BY
        1, 2, 3
)
SELECT
    COUNT(DISTINCT address) AS unique_erc20_contracts_exceeding_1k,
    ARRAY_AGG(
        STRUCT(t1.address, t1.transaction_month, t1.topic AS most_frequent_signature_hash) 
        ORDER BY t1.address, t1.transaction_month
    ) AS details_by_contract_and_month
FROM FrequentTopics AS t1
WHERE t1.topic_rank = 1
EOF
)

QUERY_PEAK_SECURITY_UNOPTIMIZED=$(cat <<'EOF'
SELECT
    t1.miner,
    t1.gas_limit
FROM
    `bigquery-public-data.crypto_ethereum_classic.blocks` AS t1
WHERE
    t1.total_difficulty = (
        -- This forces a scan of the blocks table
        SELECT
            MAX(total_difficulty)
        FROM
            `bigquery-public-data.crypto_ethereum_classic.blocks`
    )
LIMIT 1
EOF
)

QUERY_PEAK_SECURITY_OPTIMIZED=$(cat <<'EOF'
SELECT
    t1.miner,
    t1.gas_limit
FROM
    (
        SELECT
            miner,
            gas_limit,
            ROW_NUMBER() OVER (ORDER BY total_difficulty DESC) AS rank_by_difficulty
        FROM
            `bigquery-public-data.crypto_ethereum_classic.blocks`
    ) AS t1
WHERE
    t1.rank_by_difficulty = 1
LIMIT 1
EOF
)

QUERY_TOP_INTERNAL_UNOPTIMIZED=$(cat <<'EOF'
SELECT
    t.to_address,
    SUM(t.value) AS total_value_received_wei
FROM
    `bigquery-public-data.crypto_ethereum_classic.traces` AS t
WHERE
    t.trace_type = 'call'
    AND t.status = 1
    AND (t.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR t.call_type IS NULL)
    AND t.block_timestamp >= '2020-01-01 00:00:00 UTC'
    AND t.block_timestamp < '2020-01-08 00:00:00 UTC'
    AND t.to_address IS NOT NULL
GROUP BY
    t.to_address
ORDER BY
    total_value_received_wei DESC
LIMIT 10
EOF
)

QUERY_TOP_INTERNAL_OPTIMIZED=$(cat <<'EOF'
SELECT
    to_address,
    SUM(value) AS total_value_received_wei
FROM
    `bigquery-public-data.crypto_ethereum_classic.traces`
WHERE
    trace_type = 'call'
    AND status = 1
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    AND block_timestamp >= '2020-01-01 00:00:00 UTC'
    AND block_timestamp < '2020-01-08 00:00:00 UTC'
    AND to_address IS NOT NULL
GROUP BY
    to_address
QUALIFY
    ROW_NUMBER() OVER (ORDER BY total_value_received_wei DESC) <= 10
EOF
)

QUERY_TOP_SENDER_UNOPTIMIZED=$(cat <<'EOF'
WITH TopSenders AS (
    -- CTE 1: Identify the top 5 addresses based on the count of successful transactions
    SELECT
        t.from_address AS address,
        COUNT(t.hash) AS transaction_count
    FROM
        `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
    WHERE
        t.receipt_status = 1 
        AND t.block_timestamp >= '2020-01-01 00:00:00 UTC' 
        AND t.block_timestamp < '2020-04-01 00:00:00 UTC'  
    GROUP BY
        1
    ORDER BY
        transaction_count DESC
    LIMIT 5
),

TotalFees AS (
    -- CTE 2: Requires a second scan of the 'transactions' table unnecessarily
    SELECT
        t.from_address AS address,
        SUM(CAST(t.gas_price AS NUMERIC) * CAST(t.receipt_gas_used AS NUMERIC)) AS total_fees_paid
    FROM
        `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
    INNER JOIN
        TopSenders AS s ON t.from_address = s.address
    WHERE
        t.block_timestamp >= '2020-01-01 00:00:00 UTC'
        AND t.block_timestamp < '2020-04-01 00:00:00 UTC'
        AND t.receipt_status = 1
    GROUP BY
        1
),

TotalValueSent AS (
    -- CTE 3: Scans the 'traces' table
    SELECT
        tr.from_address AS address,
        SUM(tr.value) AS total_value_sent
    FROM
        `bigquery-public-data.crypto_ethereum_classic.traces` AS tr
    INNER JOIN
        TopSenders AS s ON tr.from_address = s.address
    WHERE
        tr.block_timestamp >= '2020-01-01 00:00:00 UTC'  
        AND tr.block_timestamp < '2020-04-01 00:00:00 UTC' 
    GROUP BY
        1
)

-- Final SELECT: Joins the calculated fees and values to find the net ether flow
SELECT
    f.address,
    v.total_value_sent,
    f.total_fees_paid,
    v.total_value_sent - f.total_fees_paid AS net_ether_flow_wei
FROM
    TotalFees AS f
INNER JOIN
    TotalValueSent AS v ON f.address = v.address
ORDER BY
    net_ether_flow_wei DESC
LIMIT 100
EOF
)

QUERY_TOP_SENDER_OPTIMIZED=$(cat <<'EOF'
WITH TransactionSummary AS (
    -- CTE 1 (Optimized): Single scan of the transactions table to calculate fees and rank all relevant senders.
    SELECT
        t.from_address,
        SUM(CAST(t.gas_price AS NUMERIC) * CAST(t.receipt_gas_used AS NUMERIC)) AS total_fees_paid,
        ROW_NUMBER() OVER (
            ORDER BY COUNT(t.hash) DESC
        ) AS rn_transaction_count
    FROM
        `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
    WHERE
        t.receipt_status = 1 
        AND t.block_timestamp >= '2020-01-01 00:00:00 UTC'
        AND t.block_timestamp < '2020-04-01 00:00:00 UTC'
    GROUP BY
        1
),

TraceSummary AS (
    -- CTE 2 (Optimized): Single scan of the traces table to calculate the total value sent
    SELECT
        tr.from_address,
        SUM(tr.value) AS total_value_sent
    FROM
        `bigquery-public-data.crypto_ethereum_classic.traces` AS tr
    WHERE
        tr.block_timestamp >= '2020-01-01 00:00:00 UTC'  
        AND tr.block_timestamp < '2020-04-01 00:00:00 UTC'
    GROUP BY
        1
)

-- Final SELECT: Filter to the top 5 addresses (using the rank) and join the two pre-aggregated summaries.
SELECT
    ts.from_address,
    ts.total_fees_paid,
    vs.total_value_sent,
    vs.total_value_sent - ts.total_fees_paid AS net_ether_flow_wei
FROM
    TransactionSummary AS ts
INNER JOIN
    TraceSummary AS vs ON ts.from_address = vs.from_address
WHERE
    ts.rn_transaction_count <= 5 -- Filter down to the top 5 addresses
ORDER BY
    net_ether_flow_wei DESC
LIMIT 100
EOF
)


# --- Execution Function ---

run_query() {
    local query_name=$1
    local query=$2
    echo "=================================================="
    echo "Executing $query_name"
    echo "=================================================="

    echo "Query:"
    echo "$query"
    echo "--------------------------------------------------"

    # Submit the query asynchronously and capture the output
    job_submission_output=$(bq query --nouse_cache --nosync --use_legacy_sql=false --format=json --location=US "$query" 2>&1)
    job_id=$(echo "$job_submission_output" | jq -r .jobReference.jobId 2>/dev/null)

    if [ -n "$job_id" ]; then
      echo "  [SUCCESS] Query submitted."
      echo "  *** JOB ID FOR $query_name: $job_id ***"
      echo "  Waiting for job to complete..."

      wait_output=$(bq wait "$job_id" 2>&1)
      if [ $? -ne 0 ]; then
          echo "  [FAILURE] bq wait command failed for job $job_id."
          echo "  => BQ WAIT OUTPUT:"
          echo "$wait_output"
          return
      fi

      echo "  Job completed. Fetching statistics..."
      job_stats_json=$(bq show --format=json -j "$job_id")

      total_bytes_processed=$(echo "$job_stats_json" | jq -r .statistics.totalBytesProcessed)
      total_slot_ms=$(echo "$job_stats_json" | jq -r .statistics.totalSlotMs)
      start_time=$(echo "$job_stats_json" | jq -r .statistics.startTime)
      end_time=$(echo "$job_stats_json" | jq -r .statistics.endTime)

      start_ms=$(echo "$start_time" | awk '{print int($1)}')
      end_ms=$(echo "$end_time" | awk '{print int($1)}')
      duration_ms=$((end_ms - start_ms))

      echo "  --- Execution Statistics ---"
      echo "  Duration: ${duration_ms} ms"
      echo "  Bytes Processed: ${total_bytes_processed} bytes"
      echo "  Slot Milliseconds: ${total_slot_ms}"
      echo "  ----------------------------"
      
      # Append the details to the summary file
      echo -e "${query_name}\t${job_id}\t${duration_ms}\t${total_bytes_processed}\t${total_slot_ms}" >> "$SUMMARY_FILE"

    else
      echo "  [FAILURE] Failed to submit query: $query_name."
      echo "  => BQ SUBMISSION OUTPUT:"
      echo "$job_submission_output"
    fi
    echo ""
}

# --- Run All Queries ---

echo "--- Running queries from daily_avg_value_and_fees_eval.sql ---"
run_query "Daily Avg Unoptimized" "$QUERY_DAILY_AVG_UNOPTIMIZED"
run_query "Daily Avg Optimized" "$QUERY_DAILY_AVG_OPTIMIZED"

echo "--- Running queries from erc20_high_activity_event_analysis_eval.sql ---"
run_query "ERC20 Unoptimized" "$QUERY_ERC20_UNOPTIMIZED"
run_query "ERC20 Optimized" "$QUERY_ERC20_OPTIMIZED"

echo "--- Running queries from peak_security_block_details_eval.sql ---"
run_query "Peak Security Unoptimized" "$QUERY_PEAK_SECURITY_UNOPTIMIZED"
run_query "Peak Security Optimized" "$QUERY_PEAK_SECURITY_OPTIMIZED"

echo "--- Running queries from top_internal_value_recipients_eval.sql ---"
run_query "Top Internal Unoptimized" "$QUERY_TOP_INTERNAL_UNOPTIMIZED"
run_query "Top Internal Optimized" "$QUERY_TOP_INTERNAL_OPTIMIZED"

echo "--- Running queries from top_sender_net_flow_analysis_eval.sql ---"
run_query "Top Sender Unoptimized" "$QUERY_TOP_SENDER_UNOPTIMIZED"
run_query "Top Sender Optimized" "$QUERY_TOP_SENDER_OPTIMIZED"


# Turn off command printing
set +x

echo "==========================================================="
echo "Script finished at $(date)."
