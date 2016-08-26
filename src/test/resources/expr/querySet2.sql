select
sum(extendedprice) as sum_base_price,
avg(extendedprice) as avg_price
from
lineitem_1
where
shipdate <= '1996-01-01'
and linestatus = 'F'