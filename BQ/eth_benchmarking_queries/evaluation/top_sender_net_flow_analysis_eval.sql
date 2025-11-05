##Unoptimized Query: Redundant Scanning and Multi-Stage Aggregation

/*
This query is unoptimized because it performs redundant scans and aggregation steps on the massive transactions table.
1. The `TopSenders` CTE scans the transactions table to find the top 5 addresses by count.
2. The `TotalFees` CTE unnecessarily *re-scans* and *re-filters* the transactions table for the *same time period* and joins it back to the small `TopSenders` result.
3. This multi-CTE, multi-join structure creates unnecessary work and overhead for the query engine.

Duration 1 sec
Bytes processed 1.49 GB
Bytes billed 1.49 GB
Slot milliseconds 68930
*/

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
LIMIT 100;


## Optimized Query: Single-Pass Aggregation and Simplified Joins

/* This highly optimized query performs a single scan of the transactions table to calculate multiple metrics.
1. `TransactionSummary` calculates fees and simultaneously assigns a rank to every address using a window function (ROW_NUMBER()) in a single aggregation step, eliminating the redundant scans and joins of the unoptimized version.
2. The final result joins two pre-aggregated, minimal CTEs, significantly reducing the computational overhead (Slot Milliseconds) compared to the unoptimized three-CTE join flow.

Duration 0 sec
Bytes processed 1.49 GB
Bytes billed 1.49 GB
Slot milliseconds 18761
*/

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
LIMIT 100;