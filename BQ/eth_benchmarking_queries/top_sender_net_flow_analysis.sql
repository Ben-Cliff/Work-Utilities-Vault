/*
Query Name: top_sender_net_flow_analysis

The query performs a complex, multi-table ledger reconciliation for the top 5 
most active senders of successful transactions during Q1 2020.

It calculates the NET ETHER FLOW by:
1. Identifying the top 5 senders by successful transaction count.
2. Summing the total value sent by those users (from the 'traces' table).
3. Summing the total fees paid by those users (from the 'transactions' table).
4. Subtracting Total Fees from Total Value Sent (Net Ether Flow).

Execution details:
Duration - 1 sec
Bytes processed - 1.49 GB
Bytes billed - 1.49 GB
Slot milliseconds - 83958

*/

WITH TopSenders AS (
    -- CTE 1: Identify the top 5 addresses based on the count of successful transactions
    SELECT
        t.from_address AS address,
        COUNT(t.hash) AS transaction_count
    FROM
        `bigquery-public-data.crypto_ethereum_classic.transactions` AS t
    WHERE
        t.receipt_status = 1 -- Successful transactions 
        AND t.block_timestamp >= '2020-01-01 00:00:00 UTC' -- Q1 start 
        AND t.block_timestamp < '2020-04-01 00:00:00 UTC'  -- Q1 end 
    GROUP BY
        1
    ORDER BY
        transaction_count DESC
    LIMIT 5
),

TotalFees AS (
    -- CTE 2: Calculate the total fees paid by the top 5 addresses
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
    -- CTE 3: Calculate the total Ether value sent by the top 5 addresses via traces/internal calls
    SELECT
        tr.from_address AS address,
        SUM(tr.value) AS total_value_sent
    FROM
        `bigquery-public-data.crypto_ethereum_classic.traces` AS tr
    INNER JOIN
        TopSenders AS s ON tr.from_address = s.address
    WHERE
        tr.block_timestamp >= '2020-01-01 00:00:00 UTC' -- Q1 start 
        AND tr.block_timestamp < '2020-04-01 00:00:00 UTC' -- Q1 end 
    GROUP BY
        1
)

-- Final SELECT: Join the calculated fees and values to find the net ether flow
SELECT
    f.address,
    v.total_value_sent,
    f.total_fees_paid,
    -- Net Ether Flow = (Total Value Sent via traces) - (Total Transaction Fees Paid)
    v.total_value_sent - f.total_fees_paid AS net_ether_flow_wei
FROM
    TotalFees AS f
INNER JOIN
    TotalValueSent AS v ON f.address = v.address
ORDER BY
    net_ether_flow_wei DESC