-- pr trigger again
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(net_item_sales_amount) AS total_net_sales
FROM 
    {{ ref('fct_order_items') }}
GROUP BY 
    month
ORDER BY 
    month
