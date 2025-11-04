/*
Query Name: peak_security_block_details

The query identifies the block with the highest 'total_difficulty' ever recorded on the network.
It then retrieves two key details from that block:

Miner: The address that successfully mined this block.
Gas Limit: The maximum gas capacity set for this historically significant block.

Execution details:
Duration - 3 sec
Bytes processed - 756.23 MB
Bytes billed - 757 MB
Slot milliseconds- 307666
*/

WITH max_difficulty AS (
    -- 1. Determine the maximum 'total_difficulty' value across the entire 'blocks' table.
    SELECT
        MAX(total_difficulty) AS max_diff
    FROM
        `bigquery-public-data.crypto_ethereum_classic.blocks`
)
SELECT
    -- 2. Retrieve the miner and gas limit from the block(s) matching the maximum difficulty.
    t1.miner,
    t1.gas_limit
FROM
    `bigquery-public-data.crypto_ethereum_classic.blocks` AS t1,
    max_difficulty AS t2
WHERE
    t1.total_difficulty = t2.max_diff
LIMIT 1