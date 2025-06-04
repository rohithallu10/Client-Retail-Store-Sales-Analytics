 ----------------------------------------------------DATA CLEANING----------------------------------------------------------------------------------
 -- Step 1: Find sum of Total Amount from Orders table based on Order is and Customer id--(98671 rows affected)
SELECT Customer_id, Order_id, ROUND(SUM(Total_Amount), 0) AS TotalAmount 
INTO CustOrder
FROM Orders
GROUP BY Customer_id, Order_id;

-- Find sum of Payment value from Order Paymnets table based on order id--(99440 rows affected)
SELECT Order_ID, ROUND(SUM(payment_value), 0) AS PaymentTotal --(99440 rows affected)
INTO OrderPayment
FROM OrderPayments
GROUP BY Order_ID;

-- Step 2: Find Matched Orders--(88629 rows affected)
SELECT c.*
INTO MatchedOrders
FROM CustOrder c
INNER JOIN OrderPayment op 
ON c.Order_id = op.Order_ID
AND c.TotalAmount = op.PaymentTotal;


-- Step 3: Find Orders Where Payment Exists But Does Not Match Order Amount--(10811 rows affected)
SELECT op.*
INTO OrdersNotMatching
FROM OrderPayment op
LEFT JOIN CustOrder co
ON co.Order_id = op.Order_ID
AND co.TotalAmount = op.PaymentTotal
WHERE co.Customer_id IS NULL; -- Mismatched orders

-- Step 4: Get Remaining Orders That Can Be Corrected--(7268 rows affected)
SELECT o.Customer_id, o.Order_id, onm.PaymentTotal
INTO RemainingOrders
FROM OrdersNotMatching onm
INNER JOIN Orders o
ON onm.Order_ID = o.Order_id
AND onm.PaymentTotal = ROUND(o.Total_Amount, 0);


-- Step 5: Merge Matched and Corrected Orders--(95898 rows affected)
SELECT o.*
INTO NEW_ORDER_TABLE_2
FROM Orders o
INNER JOIN MatchedOrders mo
ON o.Customer_id = mo.Customer_id
AND o.Order_id = mo.Order_id

UNION ALL

SELECT o.*
FROM Orders o
INNER JOIN RemainingOrders ro
ON o.Customer_id = ro.Customer_id
AND o.Order_id = ro.Order_id
AND ro.PaymentTotal = ROUND(o.Total_Amount, 0);

-- Step 6: Create Integrated Table with Additional Information--(95898 rows affected)
SELECT * 
INTO Integrated_Table_1 
FROM (
    SELECT A.*, 
           D.Category, 
           C.Avg_rating, 
           E.seller_city, 
           E.seller_state, 
           E.Region, 
           F.customer_city, 
           F.customer_state, 
           F.Gender
    FROM NEW_ORDER_TABLE_2 A  

    -- Join order reviews to get the average rating per order
    INNER JOIN (
        SELECT A.ORDER_id, AVG(A.Customer_Satisfaction_Score) AS Avg_rating 
        FROM OrderReview_Ratings A 
        GROUP BY A.ORDER_id
    ) AS C ON C.ORDER_id = A.Order_id 

    -- Join product details
    INNER JOIN productsinfo AS D ON A.product_id = D.product_id

    -- Join store details (ensure unique store data)
    INNER JOIN (
        SELECT DISTINCT * FROM storesinfo
    ) AS E ON A.Delivered_StoreID = E.StoreID

    -- Join customer details
    INNER JOIN customers AS F ON A.Customer_id = F.Custid
) AS T;


-- Step 7: View Final Integrated Table
SELECT * FROM Integrated_Table_1;

-- Step 8: Merge Additional Orders with Different Delivery Stores--(2459 rows affected)
-- Identify Orders with Multiple Delivery Stores
SELECT DISTINCT A.*, 
       (A.Total_Amount / A.Quantity) AS Net_amount, 
       (A.Quantity / A.Quantity) AS Net_QTY  
INTO Temp_Multiple_Store_Orders
FROM Orders A
JOIN Orders B 
ON A.order_id = B.order_id 
WHERE A.Delivered_StoreID <> B.Delivered_StoreID;


-- Step 9: Merge Additional Orders into Finalized Table--(98,379)
SELECT * 
INTO Finalised_Records_no 
FROM Integrated_Table_1;

INSERT INTO Finalised_Records_no  
SELECT T.Customer_id, T.order_id, T.product_id, T.Channel, T.Delivered_StoreID, 
       T.Bill_date_timestamp, SUM(T.Net_QTY) AS Quantity, T.Cost_Per_Unit, T.MRP, 
       T.Discount, SUM(T.Net_amount) AS Total_Amount, 
       C.Category, F.Customer_Satisfaction_Score AS Avg_rating,
       G.seller_city, G.seller_state, G.Region, 
       E.customer_city, E.customer_state, E.Gender  
FROM Temp_Multiple_Store_Orders T
INNER JOIN productsinfo C ON T.product_id = C.product_id  
INNER JOIN orderpayments D ON T.order_id = D.order_id  
INNER JOIN customers E ON T.Customer_id = E.Custid  
INNER JOIN OrderReview_Ratings F ON T.order_id = F.order_id  
INNER JOIN storesinfo G ON T.Delivered_StoreID = G.StoreID  
GROUP BY T.Customer_id, T.order_id, T.product_id, T.Channel, T.Bill_date_timestamp, 
         T.Cost_Per_Unit, T.Delivered_StoreID, T.Discount, T.MRP, T.Total_Amount, 
         T.Quantity, T.Net_amount, T.Net_QTY, C.Category, 
         F.Customer_Satisfaction_Score, G.seller_city, G.seller_state, G.Region, 
         E.customer_city, E.customer_state, E.Gender;

select * from Finalised_Records_no

-- Step 10: Create the `Add_records` Table (Fixing the Order of Operations)--(2326 rows affected)
-- This stores additional orders that had multiple delivery stores (created before using it in Step 11)
SELECT * 
INTO Add_records 
FROM (
    SELECT T.Customer_id, T.order_id, T.product_id, T.Channel, T.Delivered_StoreID, 
           T.Bill_date_timestamp, SUM(T.Net_QTY) AS Quantity, T.Cost_Per_Unit, 
           T.MRP, T.Discount, SUM(Net_amount) AS Total_Amount, 
           C.Category, F.Customer_Satisfaction_Score AS Avg_rating,
           G.seller_city, G.seller_state, G.Region, 
           E.customer_city, E.customer_state, E.Gender
    FROM Temp_Multiple_Store_Orders T
    INNER JOIN productsinfo C ON T.product_id = C.product_id
    INNER JOIN orderpayments D ON T.order_id = D.order_id
    INNER JOIN customers E ON T.Customer_id = E.Custid
    INNER JOIN OrderReview_Ratings F ON T.order_id = F.order_id
    INNER JOIN storesinfo G ON T.Delivered_StoreID = G.StoreID
    GROUP BY T.Customer_id, T.order_id, T.product_id, T.Channel, T.Bill_date_timestamp, 
             T.Cost_Per_Unit, T.Delivered_StoreID, T.Discount, T.MRP, 
             C.Category, F.Customer_Satisfaction_Score, G.seller_city, 
             G.seller_state, G.Region, E.customer_city, E.customer_state, E.Gender
) A;


-- Step 11: Remove Duplicates and Create Finalized Records Table--(98276 rows affected)
-- Now that `Add_records` exists, we can use it to clean the final table
SELECT * 
INTO Finalised_Records_1 
FROM (
    SELECT * FROM Finalised_Records_no
    EXCEPT
    SELECT A.* FROM Add_records A
    INNER JOIN Integrated_Table_1 B
    ON A.order_id = B.order_id
) X;


-- Step 12: View Finalized Table--(98276 rows)
SELECT * FROM Finalised_Records_1
SELECT * INTO F8 FROM Finalised_Records_1

SELECT * FROM F8
where order_id = 'b50e9081ca228b1950dba3678b9e13b8'
------------------------------------DESCRIPENCIES IN FINALIZED RECORDS-----------------------------------------------------------
--Duplicates (No duplicates)
SELECT *, COUNT(*) AS Duplicate_Count
FROM F8
GROUP BY 
    Customer_id, order_id, product_id, Channel, Delivered_StoreID, 
    Bill_date_timestamp, Quantity, Cost_Per_Unit, MRP, Discount, 
    Total_Amount, Category, Avg_rating, seller_city, seller_state, 
    Region, customer_city, customer_state, Gender
HAVING COUNT(*) > 1;

--Same Order coming from Different Stores (No of Distinct Orders - 1000) (No of Orders - 2379)
SELECT COUNT(Order_ID) AS No_of_Discrepant_Orders
FROM F8
WHERE Order_ID IN (
    SELECT Order_ID
    FROM F8
    GROUP BY Order_ID
    HAVING COUNT(DISTINCT Delivered_StoreID) > 1
);
--(1198 rows effected)
WITH cte AS (    
    SELECT order_id, MIN(Delivered_StoreID) AS MinStoreID
    FROM F8
    GROUP BY order_id
)
UPDATE o
SET Delivered_StoreID = c.MinStoreID
FROM F8 o
JOIN cte c ON o.order_id = c.order_id 
AND o.Delivered_StoreID <> c.MinStoreID;

--Cumulative Descrepency (No Cumulative Descripency)
WITH RankedOrders AS (
    SELECT 
        Customer_id, 
        product_id, 
        Channel, 
        Delivered_StoreID, 
        Bill_date_timestamp, 
        Cost_Per_Unit, 
        MRP, 
        Discount, 
        Quantity, 
        Total_Amount,
        RANK() OVER (
            PARTITION BY Customer_id, product_id, Channel, Delivered_StoreID, 
                         Bill_date_timestamp, Cost_Per_Unit, MRP, Discount
            ORDER BY Quantity DESC
        ) AS rnk
    FROM F8
)
SELECT * 
FROM F8  
JOIN RankedOrders 
ON F8.Customer_id = RankedOrders.Customer_id
AND F8.product_id = RankedOrders.product_id
AND F8.Channel = RankedOrders.Channel
AND F8.Delivered_StoreID = RankedOrders.Delivered_StoreID
AND F8.Bill_date_timestamp = RankedOrders.Bill_date_timestamp
AND F8.Cost_Per_Unit = RankedOrders.Cost_Per_Unit
AND F8.MRP = RankedOrders.MRP
AND F8.Discount = RankedOrders.Discount
AND F8.Quantity = RankedOrders.Quantity
WHERE RankedOrders.rnk > 1;

--Timestamp (distinct orders where there are 2 different bill dates)
select order_id
from F8
where order_id in(
select order_id
from F8
group by order_id
having count(distinct Bill_date_timestamp)>1)

--same order id different bill date (331 rows effected)
WITH cte AS (
    SELECT order_id, 
           MAX(Bill_date_timestamp) AS latest
    FROM F8
    GROUP BY order_id
)
UPDATE o
SET Bill_date_timestamp = c.latest
FROM F8 o
JOIN cte c ON o.order_id = c.order_id
WHERE o.Bill_date_timestamp <> c.latest;

--Timestamp period certain orders are out of time period (No of such orders 3)
SELECT * 
FROM F8
WHERE Bill_date_timestamp < '2021-09-01' 
   OR Bill_date_timestamp > '2023-10-31';

--Same order id given to multiple customers (No such Descripency)
select order_id
from F8 
group by order_id
having count(distinct Customer_id)>1

--Final table view
SELECT * FROM F8
--------------
select * into P8 from ProductsInfo
--Products info (Updating #N/A Values by others)
--623 rows effected
update P8
set Category='Others'
where Category = '#N/A'

--611 rows effected
update P8
set Category='Others'
where product_name_lenght is null or
      product_description_lenght is null or 
	  product_photos_qty is null or 
	  product_weight_g is null or
	  product_length_cm is null or 
	  product_height_cm is null or
	  product_width_cm is null

--(FINALISED TABLE VIEW)
SELECT * FROM F8 
-------------------------------CREATING CUSTOMER, PRODUCT AND ORDERS 360 TABLES-----------------------------------------------------------
--Customer 360
select 
    c.custid as customer_id,
    c.customer_city,
    c.customer_state,
    c.gender,

    -- transaction dates
    min(f.bill_date_timestamp) as first_transaction_date,
    max(f.bill_date_timestamp) as last_transaction_date,
    datediff(day, min(f.bill_date_timestamp), max(f.bill_date_timestamp)) as tenure,
    datediff(day, max(f.bill_date_timestamp), (select max(bill_date_timestamp) from f8)) as inactive_days,
	COUNT(CASE WHEN DATENAME(WEEKDAY, f.Bill_date_timestamp) IN ('Saturday', 'Sunday') THEN 1 END) AS Weekend_Transactions,
    COUNT(CASE WHEN DATENAME(WEEKDAY, f.Bill_date_timestamp) NOT IN ('Saturday', 'Sunday') THEN 1 END) AS Weekday_Transactions,

    -- transaction metrics
    count(f.order_id) as total_transactions,
    count(distinct f.order_id) as total_distinct_transactions,
    sum(f.total_amount) as total_revenue,
    sum(f.quantity) as total_quantity_purchased,
    sum(f.discount) as total_discount_taken,
   (sum(f.discount) * 1.0) / sum(f.total_amount) * 100 as average_discount_percent,
    sum(f.total_amount) / count(distinct f.order_id) as average_order_value,
	sum(f.total_amount - f.cost_per_unit * f.quantity) as profit,

    -- purchase behavior
    count(distinct f.product_id) as distinct_items_purchased,
    count(distinct f.category) as distinct_categories_purchased,
    count(distinct f.delivered_storeid) as distinct_stores_purchased,
    count(distinct f.seller_city) as distinct_cities_purchased,

    -- payment metrics
    count(distinct p.payment_type) as different_payment_types_used,
    sum(case when p.payment_type = 'voucher' then 1 else 0 end) as transactions_using_voucher,
    sum(case when p.payment_type = 'credit_card' then 1 else 0 end) as transactions_using_creditcard,
    sum(case when p.payment_type = 'debit_card' then 1 else 0 end) as transactions_using_debitcard,
    sum(case when p.payment_type = 'upi/cash' then 1 else 0 end) as transactions_using_upi_or_cash

into customer_360
from customers c
join f8 f on f.customer_id = c.custid  
join orderpayments p on f.order_id = p.order_id  
group by c.custid, c.customer_city, c.customer_state, c.gender

select * from customer_360
---------------------------------------------------------------
--Orders 360
SELECT 
    f.order_id,
    f.customer_id,
    f.channel,
    f.bill_date_timestamp,

	
    -- Date Metrics
	DATENAME(MONTH, f.bill_date_timestamp) AS Order_Month, 
    DATEPART(QUARTER, f.bill_date_timestamp) AS Order_Quarter,  
    DATEPART(YEAR, f.bill_date_timestamp) AS Order_Year,

    -- Order Metrics
    COUNT(f.product_id) AS total_products_in_order,
    SUM(f.quantity) AS total_quantity_in_order,
    SUM(f.total_amount) AS total_order_value,
    SUM(f.discount) AS total_discount_on_order,
    (SUM(f.discount) * 1.0 / NULLIF(SUM(f.total_amount), 0)) * 100 AS discount_percent,
    SUM(f.total_amount - f.cost_per_unit * f.quantity) AS profit,

    -- Unique Counts
    COUNT(DISTINCT f.category) AS distinct_categories_in_order,
    COUNT(DISTINCT f.seller_city) AS distinct_seller_cities,
    COUNT(DISTINCT f.delivered_storeid) AS distinct_delivered_stores,
    COUNT(DISTINCT f.product_id) AS distinct_products_in_order,

    -- Payment Information
    COUNT(DISTINCT p.payment_type) AS different_payment_types_used,
    SUM(CASE WHEN p.payment_type = 'voucher' THEN 1 ELSE 0 END) AS transactions_using_voucher,
    SUM(CASE WHEN p.payment_type = 'credit_card' THEN 1 ELSE 0 END) AS transactions_using_creditcard,
    SUM(CASE WHEN p.payment_type = 'debit_card' THEN 1 ELSE 0 END) AS transactions_using_debitcard,
    SUM(CASE WHEN p.payment_type = 'upi/cash' THEN 1 ELSE 0 END) AS transactions_using_upi_or_cash,

    -- Product Analysis
    SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0) AS avg_selling_price_per_product

INTO orders_360
FROM f8 f
JOIN orderpayments p ON f.order_id = p.order_id
GROUP BY 
    f.order_id, 
    f.customer_id, 
    f.channel, 
    f.bill_date_timestamp;


drop table orders_360
select * from orders_360

select count(distinct order_id)
from orders_360

------------------------------------------------------------
-- Stores 360
SELECT 
    f.Delivered_StoreID,
    s.seller_city,
    s.seller_state,
    s.Region,

    -- Store-Level Metrics
    COUNT(DISTINCT f.order_id) AS Total_Orders_Fulfilled,
    COUNT(DISTINCT f.Customer_id) AS Unique_Customers_Served,
    COUNT(DISTINCT f.product_id) AS Distinct_Products_Sold,
    COUNT(DISTINCT f.Category) AS Distinct_Categories_Sold,
    
    -- Sales Performance
    SUM(f.Quantity) AS Total_Quantity_Sold,
    SUM(f.Total_Amount) AS Total_Revenue,
    SUM(f.Discount) AS Total_Discount_Given,
    AVG(f.Total_Amount) AS Avg_Order_Value,
    
    -- Store Activity Metrics
    MIN(f.Bill_date_timestamp) AS First_Order_Date,
    MAX(f.Bill_date_timestamp) AS Last_Order_Date,
    DATEDIFF(DAY, MIN(f.Bill_date_timestamp), MAX(f.Bill_date_timestamp)) AS Store_Active_Tenure,

    -- Store Inactive Days 
    DATEDIFF(DAY, 
        MAX(f.Bill_date_timestamp), 
        (SELECT MAX(Bill_date_timestamp) FROM F8)
    ) AS Store_Inactive_Days

INTO Store_360
FROM F8 f
LEFT JOIN StoresInfo s ON f.Delivered_StoreID = s.StoreID
GROUP BY 
    f.Delivered_StoreID, 
    s.seller_city, 
    s.seller_state, 
    s.Region;

select * from Store_360

------------------------------------------------DATA ANALYSIS-------------------------------------------------------------------
--Order level Metrics
-- Total number of unique orders placed
select count(distinct order_id) as total_unique_orders from orders_360;

-- Total quantity of all items sold
select sum(total_quantity_in_order) as total_quantity from orders_360;

-- Average revenue per order
select avg(total_order_value) as avg_order_value from orders_360;

-- Average number of items per order
select avg(1.0*total_quantity_in_order) as avg_items_per_order from orders_360;

-- Average number of distinct categories in each order
select avg(1.0*distinct_categories_in_order) as avg_categories_per_order from orders_360;

--Percentage of single-category orders
SELECT 
  100.0 * COUNT(CASE WHEN distinct_categories_in_order = 1 THEN 1 END) / COUNT(DISTINCT order_id) AS pct_single_category_orders
FROM orders_360;

-- percentage of multi-category orders
SELECT
  100.0 * COUNT(CASE WHEN distinct_categories_in_order > 1 THEN 1 END) / COUNT(DISTINCT order_id) AS pct_multi_category_orders
FROM orders_360;

---------------------------------------------------------------------------------------------------------------------------------------
--Customer level Metrics
-- Total number of unique customers
select count(distinct custid) as total_customers from customer_360;

-- Average number of orders placed per customer
select avg(1.0*total_distinct_transactions) as avg_transactions_per_customer from customer_360;

-- Average amount spent per customer
select avg(total_revenue) as avg_sales_per_customer from customer_360;

-- Average profit generated per customer
select avg(profit) as avg_profit_per_customer from customer_360;

-- Percentage of customers with more than one purchase (repeat buyers)
select 
    count(*) * 100.0 / (select count(*) from customer_360) as repeat_purchase_rate
from customer_360
where total_distinct_transactions > 1;

-- Percentage of customers who purchased only once
select 
    count(*) * 100.0 / nullif((select count(*) from customer_360), 0) as one_time_buyer_percentage
from customer_360
where total_distinct_transactions = 1;

------------------------------------------------------------------------------------------------------------------------
--Discount & Profit Metrics
select sum(total_order_value) as total_revenue from orders_360;

select sum(total_discount_on_order) as total_discount from orders_360;

select avg(1.0*total_discount_on_order) as avg_discount_per_order from orders_360;

select avg(discount_percent) as avg_discount_percent from orders_360;

select sum(profit) as total_profit from orders_360;

select sum(total_order_value - profit) as total_cost from orders_360;

select (sum(total_discount_on_order) * 100.0) / (sum(total_order_value)) as discount_percentage from orders_360;

select (sum(profit) * 100.0) / (sum(total_order_value)) as profit_percentage from orders_360;

--------------------------------------------------------------------------------------------------------------------------------------
--Store level
-- Total number of unique stores fulfilling orders
select count(distinct Delivered_StoreID) as total_stores from store_360;

-- Total number of unique cities with stores
select count(distinct seller_city) as total_cities from store_360;

-- Total number of unique states with stores
select count(distinct seller_state) as total_states from store_360;

-- Total number of unique sales regions
select count(distinct region) as total_regions from store_360;

-- Total number of unique sales channels (e.g., online, offline)
select count(distinct channel) as total_channels from orders_360;

-----------------------------------------------------------------------------------------------------------------------
--Product & Category Metrics
select sum(Quantity) as Total_Quantity_sold from F8

select count(distinct product_id) as total_products from f8;

select count(distinct category) as total_categories from f8;

-- Total weekend and weekday transactions across all customers
select 
    sum(Weekend_Transactions) as total_weekend_transactions,
    sum(Weekday_Transactions) as total_weekday_transactions
from customer_360;

------------------------------------------------------------------------------------------------------------------------
--new customers acquired every month and year
select year(first_transaction_date) as year,
       datename(month,first_transaction_date) as month,
	   count(distinct custid) as New_customers
from customer_360
where year(first_transaction_date) in ('2021','2022','2023')
group by year(first_transaction_date) ,
       datename(month,first_transaction_date),
	    month(first_transaction_date)
order by year(first_transaction_date) ,
       month(first_transaction_date)

--state wise revenue
select seller_state,sum(Total_Amount) as total_revenue
from F8
group by seller_state
order by total_revenue desc

--revenue generated from each region
select Region,sum(Total_Amount) as revenue
from F8
group by Region
order by revenue desc

--popular categories by region
with cte as(
select Region,Category,count(order_id) as Order_count,
ROW_NUMBER() over(partition by Region order by count(order_id) desc) as rn
from F8
group by Category,Region)

select Region,Category,Order_count,rn
from cte
where rn<=5

--popular categories by state 
WITH cte AS (
    SELECT 
        seller_state,                   -- Correct column name for state
        Category, 
        COUNT(order_id) AS Order_count,
        ROW_NUMBER() OVER (PARTITION BY seller_state ORDER BY COUNT(order_id) DESC) AS rn  -- Corrected to seller_state
    FROM F8
    GROUP BY seller_state, Category  -- Corrected to seller_state
)

SELECT 
    seller_state,                     -- Correct column name for state
    Category, 
    Order_count, 
    rn
FROM cte
WHERE rn <= 5;  -- Top 5 categories per seller state



--sales by channel
select Channel,sum(Quantity) as Total_Quantity,sum(Total_Amount) as Revenue
from F8
group by Channel
order by Total_Quantity desc
--contribution to revenue
SELECT 
    Channel,
    SUM(Quantity) AS Total_Quantity,
    SUM(Total_Amount) AS Revenue,
    -- Percentage contribution of Quantity
    (SUM(Quantity) * 100.0 / (SELECT SUM(Quantity) FROM F8)) AS Quantity_Percentage,
    -- Percentage contribution of Revenue
    (SUM(Total_Amount) * 100.0 / (SELECT SUM(Total_Amount) FROM F8)) AS Revenue_Percentage
FROM F8
GROUP BY Channel
ORDER BY Total_Quantity DESC;

--top 10 stores by revenue
select top 10 Delivered_StoreID,sum(Total_Amount) Revenue
from F8
group by Delivered_StoreID
order by  Revenue desc

--percentage contributed by top 10 stores in Total Revenue
WITH Top10Stores AS (
    SELECT TOP 10 
        Delivered_StoreID,
        SUM(Total_Amount) AS Revenue
    FROM F8
    GROUP BY Delivered_StoreID
    ORDER BY Revenue DESC
)
SELECT 
    SUM(Revenue) AS Top10_Total_Revenue,
    ROUND((SUM(Revenue) * 100.0) / (SELECT SUM(Total_Amount) FROM F8), 2) AS Top10_Contribution_Percentage
FROM 
    Top10Stores;


--average order value
SELECT top 10
    Delivered_StoreID,
    SUM(Total_Amount) AS Total_Revenue,
    COUNT(Order_ID) AS Total_Orders,
    SUM(Total_Amount) * 1.0 / COUNT(Order_ID) AS Average_Order_Value
FROM F8
GROUP BY Delivered_StoreID
ORDER BY Average_Order_Value DESC

--contribution by each store 
WITH Top10Stores AS (
    SELECT TOP 10
        Delivered_StoreID,
        SUM(Total_Amount) AS Revenue,
        COUNT(Order_ID) AS Total_Orders,
        SUM(Total_Amount) * 1.0 / COUNT(Order_ID) AS Average_Order_Value
    FROM F8
    GROUP BY Delivered_StoreID
    ORDER BY Average_Order_Value DESC
)
SELECT Delivered_StoreID,
    Revenue,
   ROUND((SUM(Revenue) * 100.0) / (SELECT SUM(Total_Amount) FROM F8), 2) AS Top10_Contribution_Percentage
FROM Top10Stores
group by Delivered_StoreID, Revenue

--contribution by total revenue
WITH Top10Stores AS (
    SELECT TOP 10
        Delivered_StoreID,
        SUM(Total_Amount) AS Total_Revenue,
        COUNT(Order_ID) AS Total_Orders,
        SUM(Total_Amount) * 1.0 / COUNT(Order_ID) AS Average_Order_Value
    FROM F8
    GROUP BY Delivered_StoreID
    ORDER BY Average_Order_Value DESC
)
SELECT 
    SUM(Total_Revenue) AS Top10_Total_Revenue,
    ROUND((SUM(Total_Revenue) * 100.0) / (SELECT SUM(Total_Amount) FROM F8), 2) AS Top10_Contribution_Percentage
FROM 
    Top10Stores;


--
--no of customers in each state
select customer_state,count(custid) as No_of_Customers
from customer_360
group by customer_state
order by No_of_Customers desc


--gender customer count
SELECT 
  Gender, 
  COUNT(DISTINCT Customer_id) AS Customer_Count,
  ROUND(
    COUNT(DISTINCT Customer_id) * 100.0 / 
    (SELECT COUNT(DISTINCT Customer_id) FROM F8), 2
  ) AS Percentage
FROM F8
GROUP BY Gender

--
--gender wise revenue
SELECT 
  Gender, 
  SUM(Total_Amount) AS Revenue,
  ROUND(
    SUM(Total_Amount) * 100.0 / 
    (SELECT SUM(Total_Amount) FROM F8), 2
  ) AS Percentage
FROM F8
GROUP BY Gender


--customer segementation by revenue customer 360
with cte as (
select custid,
       total_revenue,
	   PERCENT_RANK() over(order by total_revenue)*100 as percentile
from customer_360),

 prs as(
select custid,
       total_revenue,
	   percentile,
	   case when percentile > 90 then 'Premium'
	        when percentile > 70 then 'Gold'
			when percentile > 40 then 'Silver'
			else 'Standard'
			end as segment
from cte)

select segment,
       count(*) as No_of_customers,
	   sum(total_revenue) as total_revenue
from prs
group by segment
order by total_revenue desc


--customer sementation by profit
with cte as(
select custid,
       profit,
	   PERCENT_RANK() over(order by profit)*100 as percentile
from customer_360),

 d as (
select custid,
       profit,
	   percentile,
	   case when percentile > '90' then 'Platinum'
	    when percentile > '70' then 'Gold'
	    when percentile > '40' then 'Silver'
	   else 'Bronze'
	   end as segment
from cte)

select segment,
      count(*) as no_of_customers,
	  sum(profit) as Total_profit
from d
group by segment
order by Total_profit desc


-- RFM segmentation
WITH rfm_base AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, MAX(Bill_date_timestamp), (SELECT MAX(Bill_date_timestamp) FROM F8)) AS Recency,
        COUNT(DISTINCT order_id) AS Frequency,
        SUM(Total_Amount) AS Monetary
    FROM F8
    GROUP BY Customer_id
),
rfm_scores AS (
    SELECT 
        Customer_id,
        Recency,
        Frequency,
        Monetary,
        NTILE(4) OVER (ORDER BY Recency ASC) AS Recency_Score,       
        NTILE(4) OVER (ORDER BY Frequency DESC) AS Frequency_Score,  
        NTILE(4) OVER (ORDER BY Monetary DESC) AS Monetary_Score    
    FROM rfm_base
),
rfm_segmented AS (
    SELECT *,

        (Recency_Score + Frequency_Score + Monetary_Score) AS Total_RFM_Score,

        CASE 
            WHEN (Recency_Score + Frequency_Score + Monetary_Score) >= 11 THEN 'Premium'  
            WHEN (Recency_Score + Frequency_Score + Monetary_Score) >= 10 THEN 'Gold'    
            WHEN (Recency_Score + Frequency_Score + Monetary_Score) >= 9 THEN 'Silver'   
            ELSE 'Bronze'                                                             
        END AS Segment
    FROM rfm_scores
)

-- Final result: Count and Revenue per segment
SELECT 
    Segment,
    COUNT(*) AS Customer_Count,
    SUM(Monetary) AS Total_Revenue
FROM rfm_segmented
GROUP BY Segment
ORDER BY Total_Revenue DESC;


-- no of discount seekers
SELECT 
    COUNT(*) AS discount_customers_count
FROM 
    customer_360
WHERE 
    total_discount_taken > 0;

--no of discount seekers state wise
SELECT customer_state, COUNT(*) AS discount_customers_count,sum(total_revenue) as Revenue
FROM customer_360
WHERE total_discount_taken > 0
GROUP BY customer_state
ORDER BY discount_customers_count DESC;

--no of non discount seekers state wise
SELECT customer_state, COUNT(*) AS Non_discount_customers_count
FROM customer_360
WHERE total_discount_taken = 0
GROUP BY customer_state
ORDER BY Non_discount_customers_count DESC;

--Non Discount seekers
SELECT 
    COUNT(*) AS discount_customers_count
FROM 
    customer_360
WHERE 
    total_discount_taken = 0;


--percenatge revenue contributed by discount seekers
-- For Discount Seekers
WITH total_revenue_cte AS (
    SELECT SUM(total_revenue) AS total_revenue
    FROM customer_360
)
SELECT 
    COUNT(*) AS discount_customers_count,
    SUM(total_revenue) AS discount_customers_revenue,
    (SUM(total_revenue) * 100.0) / (SELECT total_revenue FROM total_revenue_cte) AS discount_revenue_percentage
FROM 
    customer_360
WHERE 
    total_discount_taken > 0;


--percenatge revenue contributed by Non discount seekers
WITH total_revenue_cte AS (
    SELECT SUM(total_revenue) AS total_revenue
    FROM customer_360
)
SELECT 
    COUNT(*) AS non_discount_customers_count,
    SUM(total_revenue) AS non_discount_customers_revenue,
    (SUM(total_revenue) * 100.0) / (SELECT total_revenue FROM total_revenue_cte) AS non_discount_revenue_percentage
FROM 
    customer_360
WHERE 
    total_discount_taken = 0;


--one time buyers by each state
SELECT customer_state, COUNT(*) AS one_time_buyers_count,sum(total_revenue) as Total_Revenue_Generated
FROM customer_360
WHERE total_distinct_transactions = 1
GROUP BY customer_state
ORDER BY one_time_buyers_count DESC;

--Repeat buyers by each state
SELECT customer_state, COUNT(*) AS Repeat_buyers_count,sum(total_revenue) as Total_Revenue_Generated
FROM customer_360
WHERE total_distinct_transactions > 1
GROUP BY customer_state
ORDER BY Repeat_buyers_count DESC;

--no of customers ,revenue by one time buyers
select count(*) as No_of_Customers,sum(total_revenue) as Total_Revenue,
sum(total_revenue)*100/(select sum(total_revenue) from customer_360) as Percentage_revenue
from customer_360
where total_distinct_transactions=1

--no of customers ,revenue by Repeat buyers
select count(*) as No_of_Customers,sum(total_revenue) as Total_Revenue,
sum(total_revenue)*100/(select sum(total_revenue) from customer_360) as Percentage_Contributed
from customer_360
where total_distinct_transactions>1

--top 10 expensive products
select top 10 product_id,MRP
from F8
order by MRP desc

-----contribution in total revenue
WITH Top10Products AS (
    SELECT TOP 10 
        product_id, 
        MRP
    FROM F8
    ORDER BY MRP DESC
)

SELECT 
    SUM(F8.Total_Amount) AS total_revenue_from_top_10,
    (SUM(F8.Total_Amount) * 100.0 / (SELECT SUM(Total_Amount) FROM F8)) AS contribution_percentage
FROM F8
WHERE F8.product_id IN (SELECT product_id FROM Top10Products);

--avg monthly revenue
SELECT 
    MONTH(Bill_date_timestamp) AS month_number,
    DATENAME(MONTH, Bill_date_timestamp) AS month_name,
    AVG(total_amount) AS avg_monthly_revenue
FROM F8
GROUP BY 
    MONTH(Bill_date_timestamp), 
    DATENAME(MONTH, Bill_date_timestamp)
ORDER BY month_number;

--year wise monthly revenue
select order_year,order_month,sum(total_order_value) as revenue
from orders_36
group by order_year,order_month
order by order_year,order_month




