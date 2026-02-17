CREATE STREAMING TABLE orders_bronze
as
SELECT *,
_metadata.file_name as filename,
current_timestamp() as load_time
FROM cloud_files('abfss://retail@mypracticesa101.dfs.core.windows.net/input/orders/', 'csv', map("cloudFiles.inferColumnTypes","True"));

CREATE STREAMING TABLE customers_bronze
as
SELECT *,
_metadata.file_name as filename,
current_timestamp() as load_time
FROM cloud_files('abfss://retail@mypracticesa101.dfs.core.windows.net/input/customers/', 'csv', map("cloudFiles.inferColumnTypes","True"));

CREATE STREAMING TABLE payments_bronze
as
SELECT *,
_metadata.file_name as filename,
current_timestamp() as load_time
FROM cloud_files('abfss://retail@mypracticesa101.dfs.core.windows.net/input/payments/', 'csv', map("cloudFiles.inferColumnTypes","True"));


CREATE STREAMING TABLE orders_silver_cleaned (
	CONSTRAINT valid_order EXPECT (orderid is NOT NULL) ON VIOLATION DROP ROW,
	CONSTRAINT valid_customer EXPECT (customerid is NOT NULL) ON VIOLATION DROP ROW	
) as 
SELECT *
FROM STREAM(orders_bronze);


CREATE STREAMING TABLE customers_silver_cleaned(
	CONSTRAINT valid_customer EXPECT (customer_id is NOT NULL) ON VIOLATION DROP ROW,
	CONSTRAINT valid_customer_name EXPECT (customer_name is NOT NULL) ON VIOLATION DROP ROW
) as
SELECT *
from STREAM(customers_bronze);


CREATE STREAMING TABLE payments_silver_cleaned(
	CONSTRAINT valid_payment EXPECT (payment_id is NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_mode EXPECT (payment_mode IN ('CARD','UPI','CASH')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_status EXPECT (payment_status IN ('SUCCESS','FAILED','PENDING')) ON VIOLATION DROP ROW
) as
SELECT *
from STREAM(payments_bronze);


CREATE STREAMING TABLE orders_silver;

CREATE FLOW orders_silver_flow AS AUTO CDC
into orders_silver
from STREAM(orders_silver_cleaned)
keys(orderid)
sequence by load_time;

CREATE STREAMING TABLE customers_silver;

CREATE FLOW customers_silver_flow AS AUTO CDC
into customers_silver
from STREAM(customers_silver_cleaned)
keys(customer_id)
sequence by load_time
stored as scd type 2;

CREATE STREAMING TABLE payments_silver;

CREATE FLOW payments_silver_flow AS AUTO CDC
into payments_silver
from STREAM(payments_silver_cleaned)
keys(payment_id)
sequence by load_time;


create materialized view top_customers_gold as
select c.customer_id, c.customer_name,
count(distinct(o.orderid)) as total_closed_orders,
sum(o.order_amount) as total_spent,
max(o.orderdate) as last_order_date,
'SUCCESS' as payment_status 
from customers_silver c join orders_silver o 
on c.customer_id = o.customerid
join payments_silver p
on o.orderid = p.order_id
where o.order_status = 'COMPLETED'
and p.payment_status = 'SUCCESS'
group by c.customer_id, c.customer_name;




