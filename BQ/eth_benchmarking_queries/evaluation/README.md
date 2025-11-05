# Query Evaluations

This section provides a summary of the performance analysis of the unoptimized and optimized queries. The full analysis, including the queries and their performance metrics, can be found in the `*_eval.sql` files.

### `daily_avg_value_and_fees`

*   **Unoptimized:** Uses a correlated subquery, forcing a double full scan of the transactions table.
*   **Optimized:** Uses a CTE to determine the max date first, allowing BigQuery to push the filter down and scan only the necessary data.
*   **Efficiency Gains:**
    *   **Duration:** 33.3% improvement
    *   **Bytes Billed:** No change
    *   **Slot Milliseconds:** 137.9% regression
    *   *Note: While the query duration improved, the slot milliseconds regressed. This is because the optimized query, while faster, required more concurrent resources to execute.*

### `erc20_high_activity_event_analysis`

*   **Unoptimized:** Uses repeated logic and late filtering, leading to re-scanning and re-computation of intermediate results.
*   **Optimized:** Employs a multi-stage CTE pattern to filter the massive tables early and break down the complex logic.
*   **Efficiency Gains:**
    *   **Duration:** 33.3% improvement
    *   **Bytes Billed:** No change
    *   **Slot Milliseconds:** 81.3% regression
    *   *Note: Similar to the previous query, the optimized version is faster but consumes more resources.*

### `peak_security_block_details`

*   **Unoptimized:** Uses a subquery in the `WHERE` clause, forcing two separate, expensive full table scans.
*   **Optimized:** Uses the `ROW_NUMBER()` window function to perform aggregation and selection in a single pass.
*   **Efficiency Gains:**
    *   **Duration:** 40% improvement
    *   **Bytes Billed:** No change
    *   **Slot Milliseconds:** 20.7% improvement

### `top_internal_value_recipients`

*   **Unoptimized:** Uses a global `ORDER BY ... DESC LIMIT 10`, forcing a resource-intensive global sort.
*   **Optimized:** Uses the `QUALIFY` clause with a window function to perform the Top-N filtering more efficiently in parallel.
*   **Efficiency Gains:**
    *   **Duration:** 30% improvement
    *   **Bytes Billed:** No change
    *   **Slot Milliseconds:** 32.3% improvement

### `top_sender_net_flow_analysis`

*   **Unoptimized:** Performs redundant scans and aggregation steps on the transactions table.
*   **Optimized:** Performs a single scan of the transactions table to calculate multiple metrics and uses a window function for ranking.
*   **Efficiency Gains:**
    *   **Duration:** 100% improvement
    *   **Bytes Billed:** No change
    *   **Slot Milliseconds:** 72.8% improvement
