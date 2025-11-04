/*
Query Name: erc20_high_activity_event_analysis

The query identifies high-usage ERC20 contracts (based on >1,000 logs/month) and pinpoints 
their most frequently emitted event signature (the event hash found in topics[0]).

It involves complex steps:
1. Filtering the 'contracts' table for ERC20 status[cite: 24].
2. Joining with the massive 'logs' table to count monthly events[cite: 20, 23].
3. Using the UNNEST function with OFFSET to flatten the REPEATED 'topics' array and isolate the event signature hash (index 0).
4. Applying a Window Function (ROW_NUMBER) to rank the frequency of these event signatures.

Execution details:
Duration - 5 sec
Bytes processed - 5.17 GB
Bytes billed - 5.17 GB
Slot milliseconds - 1196491


*/
WITH ERC20_Contracts AS (
    -- CTE 1: Identify all unique ERC20 contract addresses
    SELECT
        address
    FROM
        `bigquery-public-data.crypto_ethereum_classic.contracts`
    WHERE
        is_erc20 = TRUE [cite: 24]
),

MonthlyLogCounts AS (
    -- CTE 2: Count the total log events emitted by each ERC20 contract per month
    SELECT
        t1.address,
        FORMAT_DATE('%Y-%m', t1.block_timestamp) AS transaction_month, [cite: 23]
        COUNT(t1.log_index) AS log_event_count [cite: 20]
    FROM
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t1
    INNER JOIN
        ERC20_Contracts AS t2 ON t1.address = t2.address
    GROUP BY
        1, 2
    HAVING
        log_event_count > 1000 -- Filter for the condition: > 1,000 log events
),

FrequentTopics AS (
    -- CTE 3: UNNEST the topics array for the contracts identified in CTE 2 and find the most frequent
    SELECT
        t1.address,
        t1.transaction_month,
        topic,
        ROW_NUMBER() OVER (PARTITION BY t1.address, t1.transaction_month ORDER BY COUNT(topic) DESC) AS topic_rank
    FROM
        MonthlyLogCounts AS t1
    INNER JOIN
        `bigquery-public-data.crypto_ethereum_classic.logs` AS t2 
        ON t1.address = t2.address
        AND FORMAT_DATE('%Y-%m', t2.block_timestamp) = t1.transaction_month
    
    -- FIX: Use WITH OFFSET to access the index of the unnested element
    CROSS JOIN
        UNNEST(t2.topics) AS topic WITH OFFSET AS topic_index [cite: 22]
    
    -- Filter on the topic_index (0 is the event signature hash)
    WHERE
        topic_index = 0
    GROUP BY
        1, 2, 3
)

-- Final SELECT: Count the unique contracts/months and display the most frequent topic hash
SELECT
    COUNT(DISTINCT address) AS unique_erc20_contracts_exceeding_1k,
    ARRAY_AGG(
        STRUCT(
            t1.address, 
            t1.transaction_month, 
            t1.topic AS most_frequent_signature_hash
        ) 
        ORDER BY t1.address, t1.transaction_month
    ) AS details_by_contract_and_month
FROM
    FrequentTopics AS t1
WHERE
    t1.topic_rank = 1