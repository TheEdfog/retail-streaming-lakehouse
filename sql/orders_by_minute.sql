SELECT
    date_trunc('minute', event_time) AS minute,
    store_id,
    count(*) AS paid_orders,
    sum(amount) AS revenue
FROM iceberg.retail.orders
WHERE status = 'paid'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
