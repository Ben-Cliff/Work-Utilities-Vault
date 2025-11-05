# Unoptimized Query: Double Scan for MAX Value

/*
This query is unoptimized because it uses a subquery in the `WHERE` clause to find the maximum difficulty. This forces the BigQuery engine to perform two separate, expensive full table scans over the entire `blocks` table: once to compute the `MAX(total_difficulty)` value, and a second time to filter the rows based on that maximum value. This **double table scan** unnecessarily consumes more computational resources (slot time).

Duration 5 sec
Bytes processed 756.23 MB
Bytes billed 757 MB
Slot milliseconds 279810
*/

```sql
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
LIMIT 1;


/* This highly efficient query uses the window function ROW_NUMBER() OVER (ORDER BY ...) to perform both the aggregation (finding the maximum) and the row selection in a single pass over the data. This canonical BigQuery optimization technique minimizes I/O and significantly reduces the total computational effort (slot time) compared to the double-scan method.

Duration 3 sec 
Bytes processed 756.23 MB 
Bytes billed 757 MB 
Slot milliseconds 221928 */

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