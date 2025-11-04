/*

The query determines the most recent date found in the entire transactions table, then uses that date to calculate two averages:

Average Gas Price (in Wei): The average price paid per unit of gas by all transactions on that day.

Average Transaction Value (in Wei): The average amount of Ether (in Wei) transferred by the transactions on that day.

Execution details:
Duration - 2 sec
Bytes processed -1.89 GB
Bytes billed - 1.89 GB
Slot milliseconds - 554847
*/

WITH max_date AS (
    -- 1. Determine the date of the MOST RECENT full day available in the dataset.
    SELECT MAX(DATE(block_timestamp)) AS max_dt
    FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
)
SELECT
    -- 2. Calculate the average gas price. Gas price is in Wei (INTEGER ), 
    -- and casting to NUMERIC ensures accurate high-precision average calculation.
    AVG(CAST(t.gas_price AS NUMERIC)) AS avg_gas_price_wei,

    -- 3. Calculate the average transaction value. Value is already in NUMERIC type.
    AVG(t.value) AS avg_transaction_value_wei
FROM
    `bigquery-public-data.crypto_ethereum_classic.transactions` AS t,
    max_date AS m
WHERE
    -- 4. Filter the main table scan ONLY for that most recent full day.
    DATE(t.block_timestamp) = m.max_dt