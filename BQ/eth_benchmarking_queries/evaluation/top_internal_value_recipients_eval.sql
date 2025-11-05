# Unoptimized Query: Global Sort for Top-N

/*
This query uses a global `ORDER BY ... DESC LIMIT 10` clause. In a massive table, this pattern forces the query to perform a full global sort of the aggregated results before the top 10 can be finalized and returned. This sort operation is highly resource-intensive and often becomes the single most expensive stage (in slot time) for Top-N problems in BigQuery.

Duration 10 sec
Bytes processed 153.5 GB
Bytes billed 154 GB
Slot milliseconds 1550000
*/


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
LIMIT 10;


## Optimized Query: Top-N with QUALIFY (Window Function)
/* This highly optimized query uses the modern BigQuery QUALIFY clause with a window function (ROW_NUMBER())
 to perform the Top-N filtering. 
 The window function assigns a rank to each aggregated group, and the QUALIFY clause filters these ranks, 
 allowing the query engine to perform the sort and filtering more efficiently and in parallel across workers. 
 This avoids the bottleneck of a single, massive global sort required by the unoptimized method.

Duration 7 sec 
Bytes processed 153.5 GB 
Bytes billed 154 GB 
Slot milliseconds 1050000 */

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