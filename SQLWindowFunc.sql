/*1- write a query to print 3rd highest salaried employee details for each department
(give preferece to younger employee in case of a tie). 
In case a department has less than 3 employees
then print the details of highest salaried employee in that department. */

select * from employee

with rnk as (
select *, dense_rank() over(partition by dept_id order by salary desc) as rn
from employee)
,cnt as (select dept_id,count(1) as no_of_emp from employee group by dept_id)
select
rnk.*
from 
rnk 
inner join cnt on rnk.dept_id=cnt.dept_id
where rn=3 or  (no_of_emp<3 and rn=1) 


---2- write a query to find top 3 and bottom 3 products by sales in each region.

select * from Orders

with ps as (
select region, product_id, SUM(sales) as T_sales 
from Orders
group by region, product_id), 
rk as (
select *, DENSE_RANK() over(partition by region order by T_sales desc) as rnk,
DENSE_RANK() over(partition by region order by T_sales asc) as l_rnk
from ps)
select region,product_id, case when rnk<=3 then 'Top3' else 'Bottom3' end as Top_Bottom
from rk
where rnk<=3 or l_rnk<=3


/*3- Among all the sub categories..
which sub category had highest month over month growth by sales in Jan 2020. */

select * from Orders


WITH cte AS (
    SELECT sub_category, DATEPART(month, order_date) AS mnth, SUM(sales) AS January_2020_sales
    FROM Orders
    WHERE DATEPART(year, order_date) = 2020 AND DATEPART(month, order_date) = 1
    GROUP BY sub_category, DATEPART(month, order_date)
),
t2 AS (
    SELECT sub_category, DATEPART(month, order_date) AS mnth, SUM(sales) AS December_2019_sales
    FROM Orders
    WHERE DATEPART(year, order_date) = 2019 AND DATEPART(month, order_date) = 12
    GROUP BY sub_category, DATEPART(month, order_date)
), 
test AS (
    SELECT cte.sub_category, cte.January_2020_sales, t2.December_2019_sales
    FROM cte
    LEFT JOIN t2 ON cte.sub_category = t2.sub_category
)

SELECT *, ROUND(((test.January_2020_sales - test.December_2019_sales) / test.December_2019_sales) * 100, 2) AS mom
FROM test
order by mom desc;

---Another Solution

with sbc_sales as (
select sub_category,format(order_date,'yyyyMM') as year_month, sum(sales) as sales
from Orders
group by sub_category,format(order_date,'yyyyMM')
)
, prev_month_sales as (select *,lag(sales) over(partition by sub_category order by year_month) as prev_sales
from sbc_sales)
select  top 1 * , (sales-prev_sales)/prev_sales as mom_growth
from prev_month_sales
where year_month='202001'
order by mom_growth desc


/*4- write a query to print top 3 products in each category
by year over year sales growth in year 2020. */

WITH cat_sales AS (
    SELECT category, product_id, DATEPART(year, order_date) AS order_year, SUM(sales) AS sales
    FROM Orders
    WHERE DATEPART(year, order_date) = 2020
    GROUP BY category, product_id, DATEPART(year, order_date)
), ps_sales AS (
    SELECT *, LAG(sales) OVER (PARTITION BY category ORDER BY order_year) AS previous_sales
    FROM cat_sales
), rnk AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY (sales-previous_sales)/previous_sales DESC) AS rk
    FROM ps_sales
)
SELECT *
FROM rnk
WHERE rk <= 3;

create table call_start_logs
(
phone_number varchar(10),
start_time datetime
);
insert into call_start_logs values
('PN1','2022-01-01 10:20:00'),('PN1','2022-01-01 16:25:00'),('PN2','2022-01-01 12:30:00')
,('PN3','2022-01-02 10:00:00'),('PN3','2022-01-02 12:30:00'),('PN3','2022-01-03 09:20:00')
create table call_end_logs
(
phone_number varchar(10),
end_time datetime
);
insert into call_end_logs values
('PN1','2022-01-01 10:45:00'),('PN1','2022-01-01 17:05:00'),('PN2','2022-01-01 12:55:00')
,('PN3','2022-01-02 10:20:00'),('PN3','2022-01-02 12:50:00'),('PN3','2022-01-03 09:40:00')
;


select * from call_start_logs

select * from call_end_logs


/*write a query to get start time and end time of each call from above 2 tables.
Also create a column of call duration in minutes. 
Please do take into account that
there will be multiple calls from one phone number 
and each entry in start table has a corresponding entry in end table. */


select s.phone_number,s.rn,s.start_time,e.end_time, datediff(minute,start_time,end_time) as duration
from 
(select *,row_number() over(partition by phone_number order by start_time) as rn  from call_start_logs) s
inner join (select *,row_number() over(partition by phone_number order by end_time) as rn  from call_end_logs) e
on s.phone_number = e.phone_number and s.rn=e.rn;
