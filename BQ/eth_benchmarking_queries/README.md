# Ethereum Classic Benchmarking Queries

This project contains a set of SQL queries designed for benchmarking against the Ethereum Classic public dataset available on Google BigQuery.

## Data Source

All queries run against the `bigquery-public-data.crypto_ethereum_classic` dataset. This dataset contains information about the Ethereum Classic blockchain, including blocks, transactions, traces, logs, and contracts.

## Tables

The queries use the following tables:

*   **`blocks`**: Contains information about each block in the blockchain, such as timestamp, block number, hash, and miner.
*   **`transactions`**: Contains information about each transaction, including hash, sender and receiver addresses, value, and gas information.
*   **`traces`**: Contains information about internal transactions or "traces" that are executed as part of a smart contract.
*   **`logs`**: Contains event data emitted by smart contracts.
*   **`contracts`**: Contains information about smart contracts, including their address, bytecode, and whether they are ERC20 or ERC721 contracts.
*   **`token_transfers`**: Contains information about ERC20 and ERC721 token transfers.

## Queries

The following queries are used for benchmarking:

*   **`daily_avg_value_and_fees.sql`**: Calculates the average gas price and average transaction value for the most recent day in the dataset.
*   **`erc20_high_activity_event_analysis.sql`**: Identifies high-usage ERC20 contracts and their most frequently emitted event signature.
*   **`peak_security_block_details.sql`**: Finds the block with the highest total difficulty and retrieves the miner and gas limit for that block.
*   **`top_internal_value_recipients.sql`**: Identifies the top 10 addresses that received the largest total Ether value from internal transactions over a specific week.
*   **`top_sender_net_flow_analysis.sql`**: Performs a net ether flow analysis for the top 5 most active senders of successful transactions during Q1 2020.

## Benchmarking

The primary purpose of these queries is to benchmark the performance of Google BigQuery when executing complex analytical queries against a large, public blockchain dataset. The execution details (duration, bytes processed, etc.) for each query are included in the query files themselves.

## Query Evaluations

This section provides a summary of the performance analysis of the unoptimized and optimized queries. The full analysis, including the queries and their performance metrics, can be found in the `*_eval.sql` files.

### `daily_avg_value_and_fees`

*   **Unoptimized:** Uses a correlated subquery, forcing a double full scan of the transactions table.
*   **Optimized:** Uses a CTE to determine the max date first, allowing BigQuery to push the filter down and scan only the necessary data.

### `erc20_high_activity_event_analysis`

*   **Unoptimized:** Uses repeated logic and late filtering, leading to re-scanning and re-computation of intermediate results.
*   **Optimized:** Employs a multi-stage CTE pattern to filter the massive tables early and break down the complex logic.

### `peak_security_block_details`

*   **Unoptimized:** Uses a subquery in the `WHERE` clause, forcing two separate, expensive full table scans.
*   **Optimized:** Uses the `ROW_NUMBER()` window function to perform aggregation and selection in a single pass.

### `top_internal_value_recipients`

*   **Unoptimized:** Uses a global `ORDER BY ... DESC LIMIT 10`, forcing a resource-intensive global sort.
*   **Optimized:** Uses the `QUALIFY` clause with a window function to perform the Top-N filtering more efficiently in parallel.

### `top_sender_net_flow_analysis`

*   **Unoptimized:** Performs redundant scans and aggregation steps on the transactions table.
*   **Optimized:** Performs a single scan of the transactions table to calculate multiple metrics and uses a window function for ranking.
