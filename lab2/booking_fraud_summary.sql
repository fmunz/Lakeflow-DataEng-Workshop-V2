CREATE OR REFRESH MATERIALIZED VIEW booking_fraud_summary
COMMENT 'Booking totals and fraud rate per payment method'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH fraud AS (
    SELECT DISTINCT booking_id
    FROM booking_fraud_flags
    WHERE flag = 'fraud'
)
SELECT
    p.payment_method,
    COUNT(*)                                                                   AS booking_count,
    ROUND(SUM(p.amount), 2)                                                    AS gross_amount,
    COUNT(f.booking_id)                                                        AS fraud_count,
    ROUND(SUM(CASE WHEN f.booking_id IS NOT NULL THEN p.amount ELSE 0 END), 2) AS fraud_amount,
    ROUND(COUNT(f.booking_id) * 100.0 / COUNT(*), 2)                           AS fraud_pct
FROM bookings_current b
JOIN payments         p ON p.booking_id = b.booking_id
LEFT JOIN fraud       f ON f.booking_id = b.booking_id
GROUP BY p.payment_method;
