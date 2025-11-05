# BigQuery Optimization Techniques for Ethereum Dataset

This document summarizes several BigQuery optimization techniques and provides examples using the public Ethereum dataset.

## 1. Selecting Unnecessary Columns (`SELECT *`)

**The Anti-Pattern:** Using `SELECT *` is one of the most common and costly mistakes in BigQuery. Because BigQuery is a columnar database, it charges based on the amount of data read from the columns you query. `SELECT *` forces BigQuery to read all data from every single column in the table, even if you only need a few.

**The Fix:** The solution is simple but highly effective: only select the specific columns you need for your analysis.

### Example

**Unoptimized:**

```sql
-- This query scans all columns in the transactions table, even though we only need two.
SELECT
  *
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE
  block_number = 1000000;
```

**Optimized:**

```sql
-- This query only scans the from_address and to_address columns, saving significant cost.
SELECT
  from_address,
  to_address
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE
  block_number = 1000000;
```

## 2. Filtering Early with Conditional Aggregation

**The Anti-Pattern:** A common but highly inefficient pattern is to join a massive table and then use a `CASE` statement inside an aggregate function (like `SUM`) to compute a value for a specific subset of data. This forces BigQuery to process the entire massive join, evaluating the condition for every single row, which is incredibly wasteful.

**The Fix:** Pre-filter and pre-aggregate the subset of data in a CTE first. This creates a very small, intermediate table. You can then `LEFT JOIN` this small table to get the result, which is orders of magnitude more efficient than processing the entire large table.

### Example

**Unoptimized:**

```sql
-- This query calculates the total transaction value and the value for a specific day in one pass.
-- This is inefficient as it processes the entire table.
SELECT
  from_address,
  SUM(value) AS total_value,
  SUM(CASE WHEN DATE(block_timestamp) = '2022-01-01' THEN value ELSE 0 END) AS value_on_jan_1st
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
GROUP BY
  from_address;
```

**Optimized:**

```sql
-- This query pre-aggregates the data for the specific day in a CTE,
-- then joins it to the main table.
WITH daily_value AS (
  SELECT
    from_address,
    SUM(value) AS value_on_jan_1st
  FROM
    `bigquery-public-data.crypto_ethereum_classic.transactions`
  WHERE
    DATE(block_timestamp) = '2022-01-01'
  GROUP BY
    from_address
)
SELECT
  t.from_address,
  SUM(t.value) AS total_value,
  d.value_on_jan_1st
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
LEFT JOIN
  daily_value AS d
  ON t.from_address = d.from_address
GROUP BY
  t.from_address,
  d.value_on_jan_1st;
```

## 3. Inefficient `ORDER BY`

**The Anti-Pattern:** Running an `ORDER BY` on a massive table without a `LIMIT` clause is extremely resource-intensive. To produce a total ordering of the data, BigQuery must gather all the rows onto a single worker for a final sort. This operation is very slow and will often fail with a "resources exceeded" error.

**The Fix:** Always pair `ORDER BY` with a `LIMIT` when working with large datasets. This allows BigQuery to perform a much more efficient, distributed "top-N" sort, where only the top results from each worker need to be gathered and sorted.

### Example

**Unoptimized:**

```sql
-- This query attempts to sort the entire transactions table by gas price, which is very expensive.
SELECT
  hash,
  gas_price
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
ORDER BY
  gas_price DESC;
```

**Optimized:**

```sql
-- This query efficiently finds the top 100 transactions with the highest gas price.
SELECT
  hash,
  gas_price
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
ORDER BY
  gas_price DESC
LIMIT 100;
```

## 4. Accidental Many-to-Many Join

**The Anti-Pattern:** This is a subtle but dangerous mistake. When you join multiple large "fact" tables on a common, non-unique key (like a date), you create a hidden Cartesian product. For each date, every transaction is joined with every block, leading to massively inflated, incorrect results and terrible performance.

**The Fix:** The correct pattern is to **aggregate before you join**. First, calculate the daily totals for each fact table in separate CTEs. Then, join the much smaller, pre-aggregated results. This ensures the final join is a simple and efficient one-to-one lookup.

### Example

**Unoptimized:**

```sql
-- This query joins the transactions and blocks tables on the block number,
-- which can lead to a many-to-many join if there are multiple transactions in a block.
SELECT
  t.hash,
  b.miner
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
JOIN
  `bigquery-public-data.crypto_ethereum_classic.blocks` AS b
  ON t.block_number = b.number;
```

**Optimized:**

```sql
-- This query first aggregates the transactions by block number,
-- and then joins the aggregated data to the blocks table.
WITH transactions_by_block AS (
  SELECT
    block_number,
    COUNT(*) AS transaction_count
  FROM
    `bigquery-public-data.crypto_ethereum_classic.transactions`
  GROUP BY
    block_number
)
SELECT
  t.block_number,
  t.transaction_count,
  b.miner
FROM
  transactions_by_block AS t
JOIN
  `bigquery-public-data.crypto_ethereum_classic.blocks` AS b
  ON t.block_number = b.number;
```

## 5. JavaScript UDF vs. Native SQL Function

**The Anti-Pattern:** While User-Defined Functions (UDFs) are powerful, using them for simple tasks that a native function can handle is inefficient. There is significant overhead in starting the JavaScript engine and passing data back and forth between the SQL and JS environments for every single row.

**The Fix:** Whenever a built-in SQL function exists for your task, use it. Native functions are written in C++ and are tightly integrated into the BigQuery engine, making them orders of magnitude faster.

### Example

**Unoptimized:**

```sql
-- This query uses a JavaScript UDF to format the block timestamp.
CREATE TEMP FUNCTION format_timestamp(ts TIMESTAMP)
RETURNS STRING
LANGUAGE js AS r"""
  return ts.toDateString();
""";

SELECT
  hash,
  format_timestamp(block_timestamp)
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
LIMIT 100;
```

**Optimized:**

```sql
-- This query uses the native FORMAT_TIMESTAMP function, which is much more efficient.
SELECT
  hash,
  FORMAT_TIMESTAMP('%c', block_timestamp)
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
LIMIT 100;
```

## 6. Data Exploration: `LIMIT` vs. `TABLESAMPLE`

**The Anti-Pattern:** Using `LIMIT` to get a quick preview of a large table is a common habit, but it can be inefficient. BigQuery may still need to perform a large scan to find and return the first 1,000 rows, leading to higher-than-expected costs and delays.

**The Fix:** For a statistically representative preview of your data, use `TABLESAMPLE SYSTEM`. This function is specifically designed for cheap and fast exploration, as it only reads a small, random percentage of the underlying data blocks.

### Example

**Unoptimized:**

```sql
-- This query uses LIMIT to get a sample of the transactions table.
SELECT
  *
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
LIMIT 1000;
```

**Optimized:**

```sql
-- This query uses TABLESAMPLE to get a random 1% sample of the transactions table.
SELECT
  *
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions` TABLESAMPLE SYSTEM (1 PERCENT);
```

## 7. Exact vs. Approximate Aggregations

**The Anti-Pattern:** Calculating an exact `COUNT(DISTINCT)` on a column with millions or billions of unique values (a "high cardinality" column) is computationally expensive. It requires significant resources to track every unique value encountered.

**The Fix:** For use cases where a highly accurate estimate is sufficient (like dashboards or general analysis), use `APPROX_COUNT_DISTINCT`. This function uses the efficient HyperLogLog++ algorithm to provide an estimate with a very small margin of error, but with much lower computational cost.

### Example

**Unoptimized:**

```sql
-- This query calculates the exact number of distinct from_addresses.
SELECT
  COUNT(DISTINCT from_address)
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`;
```

**Optimized:**

```sql
-- This query calculates an approximate number of distinct from_addresses, which is much faster.
SELECT
  APPROX_COUNT_DISTINCT(from_address)
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`;
```
