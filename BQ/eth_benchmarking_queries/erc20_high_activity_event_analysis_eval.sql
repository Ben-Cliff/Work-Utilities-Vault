

#Unoptimized Query: Repeated Logic and Filtering Late
/*
This query attempts to solve the multi-step problem in a complex, nested manner, 
often using non-performant IN clauses or requiring the query processor to repeat the
expensive aggregation logic (like calculating monthly counts) across multiple execution stages.
A common pattern is omitting the critical CTE staging layer and forcing the system to re-scan or re-compute 
intermediate results.

Duration 6 sec
Bytes processed 5.17 GB
Bytes billed 5.17 GB
Slot milliseconds 827299*/


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
WHERE t1.topic_rank = 1;




#Optimized 
/* 
The highly optimized query uses a clear multi-stage CTE pattern to filter the massive tables early and break down the complex logic.

ERC20_Contracts: Filters the small contracts dimension table once.

MonthlyLogCounts: Filters early and aggregates the massive logs table, shrinking the dataset significantly before further processing.

FrequentTopics: Joins the already tiny MonthlyLogCounts result back to the relevant subset of the logs data for the final UNNEST/ROW_NUMBER operation.

Duration 4 sec
Bytes processed 5.17 GB
Bytes billed 5.17 GB
Slot milliseconds 1500234
*/


-- The user-provided optimized query structure
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