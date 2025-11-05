## 🐌 Unoptimized Query: Late Filtering / Implicit Full Scan Join

/*
Why this is unoptimized:
Double Full Scan/Correlated Subquery: The inner subquery is executed to find MAX(DATE(block_timestamp)). Then, the outer query scans the entire transactions table again and applies the filter late in the process. This doubles the work and dramatically increases Slot Milliseconds (compute time).

Non-Sargable Predicate on block_timestamp: Applying the DATE() function directly to the block_timestamp column prevents BigQuery from using potential internal optimizations or partitions based on the raw timestamp data, potentially leading to a larger scan than necessary.

Duration 3 sec
Bytes processed 1.89 GB
Bytes billed 1.89 GB
Slot milliseconds 192066
*/

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
    );


## 🚀 Highly Optimized Query: Filter Early with CTE
/*

Filter Pushdown/Early Filtering: The use of the max_date CTE and the INNER JOIN allows BigQuery's optimizer to push the filter down to the underlying data storage. It only reads the data blocks that correspond to the single most recent day, leading to a massive reduction in Bytes Scanned and faster execution.

Decoupled Logic: Calculating the MAX(DATE) is separated from the main aggregation, improving overall query logic and efficiency.

Speed Gain (Query Time): The most notable gain is in query duration, which decreased by $33.3\%$ (a $1.5\text{x}$ speedup). This is typically the goal for interactive dashboards or reports.Cost Increase (Compute): However, the query consumed significantly more total compute power, increasing Slot Milliseconds by $137.9\%$. In BigQuery's capacity-based pricing (slots), this translates to a much higher computational cost for the improved speed.


Duration 2 sec
Bytes processed 1.89 GB
Bytes billed 1.89 GB
Slot milliseconds 457008

*/

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
    max_date AS m ON DATE(t.block_timestamp) = m.max_dt;

