/*
Query Name: top_internal_value_recipients

The query analyzes internal transfers (traces) to identify the top 10 addresses 
that received the largest total Ether value over a specific week.

It focuses specifically on:
- Successful internal call traces (trace_type = 'call' and status = 1)[cite: 3, 6].
- Excludes smart contract manipulation calls (e.g., delegatecall, staticcall)[cite: 4].
- Aggregates the 'value' received [cite: 2] [cite_start]by the destination address ('to_address')[cite: 2].

Execution details:
Duration 0 sec
Bytes processed 82.73 MB
Bytes billed 83 MB
Slot milliseconds 551
*/

SELECT
    t.to_address,
    SUM(t.value) AS total_value_received_wei
FROM
    `bigquery-public-data.crypto_ethereum_classic.traces` AS t
WHERE
    [cite_start]-- 1. Filter by trace type (internal calls only) [cite: 3]
    t.trace_type = 'call'
    
    [cite_start]-- 2. Ensure the call was successful (status = 1) [cite: 6]
    AND t.status = 1
    
    [cite_start]-- 3. Exclude smart contract manipulation call types [cite: 4]
    AND (
        t.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') 
        OR t.call_type IS NULL
    )
    
    [cite_start]-- 4. Filter for a specific high-volume week (Jan 1, 2020 - Jan 7, 2020) [cite: 6]
    AND t.block_timestamp >= '2020-01-01 00:00:00 UTC'
    AND t.block_timestamp < '2020-01-08 00:00:00 UTC'
    
    [cite_start]-- 5. Ensure the trace has a destination address [cite: 2]
    AND t.to_address IS NOT NULL

GROUP BY
    t.to_address
ORDER BY
    total_value_received_wei DESC
LIMIT 10