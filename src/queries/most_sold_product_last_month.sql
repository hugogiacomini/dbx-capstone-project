-- Most Sold Product in the Last Month
WITH 
-- Filter sales data for the last 30 days
recent_sales AS (
    SELECT
        product_id,
        product_name,
        category,
        quantity,
        order_date
    FROM dbx_interview.order_management_dev.gold_order_details
    WHERE date_format(order_date, 'yyyy-MM') = date_format(date_add(CURRENT_DATE, -30), 'yyyy-MM')
        AND order_status = 'completed' -- Only consider completed orders
),
-- Aggregate sales data by product
product_sales_summary AS (
    SELECT
        product_id,
        product_name,
        category,
        SUM(quantity) AS total_quantity_sold,
        COUNT(DISTINCT DATE(order_date)) AS days_with_sales,
        COUNT(*) AS number_of_orders,
        ROUND(AVG(quantity), 2) AS avg_quantity_per_order
    FROM recent_sales
    GROUP BY product_id, product_name, category
)
-- Select the product with the highest total quantity sold
SELECT
    product_id,
    product_name,
    category,
    total_quantity_sold,
    number_of_orders,
    days_with_sales,
    avg_quantity_per_order,
    ROUND(total_quantity_sold * 100.0 / SUM(total_quantity_sold) OVER (), 2) AS percent_of_total_sales
FROM product_sales_summary
ORDER BY total_quantity_sold DESC
LIMIT 1;

-- Alternative: Top 10 Most Sold Products
-- Uncomment to get top 10 instead of just the top product
/*
SELECT
    product_id,
    product_name,
    category,
    total_quantity_sold,
    number_of_orders,
    days_with_sales,
    avg_quantity_per_order,
    ROUND(total_quantity_sold * 100.0 / SUM(total_quantity_sold) OVER (), 2) AS percent_of_total_sales,
    RANK() OVER (ORDER BY total_quantity_sold DESC) AS sales_rank
FROM product_sales_summary
ORDER BY total_quantity_sold DESC
LIMIT 10;
*/
