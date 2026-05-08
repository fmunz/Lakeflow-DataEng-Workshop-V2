CREATE OR REFRESH MATERIALIZED VIEW sales_stats
COMMENT 'Sales KPIs grouped by product and payment method'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
    product,
    paymentMethod,
    COUNT(*)                     AS txn_count,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(totalPrice), 2)    AS gross_revenue,
    ROUND(AVG(totalPrice), 2)    AS avg_txn_value,
    COUNT(DISTINCT customerID)   AS unique_customers,
    COUNT(DISTINCT franchiseID)  AS franchises_selling
FROM sales_tx
GROUP BY product, paymentMethod;
