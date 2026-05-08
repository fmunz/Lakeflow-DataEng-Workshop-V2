CREATE OR REFRESH STREAMING TABLE payments_bronze
COMMENT 'Payments stream from samples.wanderbricks.payments'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
    payment_id,
    booking_id,
    amount,
    payment_method,
    status,
    payment_date
FROM STREAM samples.wanderbricks.payments;
