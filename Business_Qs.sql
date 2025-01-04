-- When is the peak season of our ecommerce?
    SELECT top 1 d.Season, COUNT(f.orderuser_fk) AS total_users
    FROM Dim_Date d
    JOIN Fact_orders f ON f.order_date = d.DateKey
    GROUP BY d.Season
	ORDER BY total_users DESC

-- What time users are most likely to make an order or use the ecommerce app?
SELECT top 1
    COUNT(DISTINCT o.order_id) AS total_users,
    FLOOR(f.order_time / 10000) AS order_hour
FROM Fact_orders f 
JOIN Dim_orderUser_payment o ON o.order_id_sk = f.orderuser_fk 
GROUP BY FLOOR(f.order_time / 10000)
ORDER BY total_users DESC

-- What is the preferred way to pay in the ecommerce?
SELECT top 1 u.payment_type , count(Distinct u.order_id) as total_orders
FROM Fact_orders f join Dim_orderUser_payment u
ON f.orderUser_FK = u.order_id_sk
GROUP BY u.payment_type
order by total_orders desc

-- How many installments are usually done when paying in the ecommerce?
SELECT TOP 1 u.payment_installments , count(Distinct u.order_id) as total_orders
FROM Fact_orders f join Dim_orderUser_payment u
ON f.orderUser_FK = u.order_id_sk
GROUP BY u.payment_installments 
order by total_orders desc

-- What is the average spending time for users on our ecommerce?
 WITH CTE1 AS
(
    SELECT DISTINCT  u.order_id,ABS(order_time - order_approved_time) AS diff 
    FROM Fact_orders f 
    JOIN Dim_orderUser_payment u ON f.orderUser_FK = u.order_id_sk 
)
SELECT 
    CONCAT(FLOOR(AVG(CAST(diff AS BIGINT)) / 60), ' min ', AVG(CAST(diff AS BIGINT)) % 60, ' sec') AS avg_time
FROM CTE1;

-- What is the frequency of purchases in each state?
SELECT customer_state , count(distinct order_id) as no_orders
FROM Fact_orders f JOIN Dim_orderUser_payment u
ON f.orderuser_fk=u.order_id_sk
group by customer_state

-- Which logistic route has heavy traffic in our ecommerce?

SELECT top 1
    seller_city, 
    customer_city, 
    COUNT(*) AS route_count
FROM 
fact_orders f join Dim_seller s on f.seller_fk = s.seller_id_sk 
join Dim_orderUser_payment o on o.order_id_sk = f.orderuser_fk
GROUP BY 
    seller_city, 
    customer_city
ORDER BY 
    route_count DESC;

-- How many late-delivered orders are there in our ecommerce? 

SELECT 
    COUNT(CASE WHEN delivery_date > estimated_date_delivery THEN 1 END) AS late_delivered
FROM 
    Fact_orders;

-- Are late orders affecting customer satisfaction? yes ... the most common score for the late deliverys is 1
SELECT fck.feedback_score, count(fck.feedback_score) as no_scores
FROM 
    Fact_orders f join Dim_feedback fck ON f.feedback_fk = fck.feedback_sk
where 
	delivery_date > estimated_date_delivery
group by fck.feedback_score 

-- How long is the delay for the delivery/shipping process in each state?
SELECT  
    o.customer_state,
    AVG(
        ABS(
            DATEDIFF(DAY, 
                TRY_CONVERT(DATE, CAST(f.estimated_date_delivery AS CHAR(8)), 112), 
                TRY_CONVERT(DATE, CAST(f.delivery_date AS CHAR(8)), 112)
            )
        ) + 
        ABS(
            DATEDIFF(DAY, 
                TRY_CONVERT(DATE, CAST(f.delivery_date AS CHAR(8)), 112), 
                TRY_CONVERT(DATE, CAST(f.order_approved_date AS CHAR(8)), 112)
            )
        )
    ) AS Avg_Diff
FROM 
    Fact_orders f 
JOIN 
    Dim_orderUser_payment o ON f.orderuser_fk = o.order_id_sk
GROUP BY 
    o.customer_state;


-- How long is the difference between the estimated delivery time and actual delivery time in each state?
SELECT  
    o.customer_state,
    AVG(ABS(DATEDIFF(DAY, 
        TRY_CONVERT(DATE, CAST(f.estimated_date_delivery AS CHAR(8)), 112), 
        TRY_CONVERT(DATE, CAST(f.delivery_date AS CHAR(8)), 112)
    ))) AS Diff_days
FROM 
    Fact_orders f
JOIN 
    Dim_orderUser_payment o ON f.orderuser_fk = o.order_id_sk
GROUP BY 
    o.customer_state;
