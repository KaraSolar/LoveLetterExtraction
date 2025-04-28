
DROP TABLE IF EXISTS extraction;

CREATE TABLE extraction AS
with numbers as (
select 0 as numb
union all
select 1 as numb
union all
select 2 as numb
union all
select 3 as numb
union all
select 4 as numb
union all
select 5 as numb
union all
select 6 as numb
union all
select 7 as numb
union all
select 8 as numb
union all
select 9 as numb
), series as (
select
    a.numb * 1000 + b.numb * 100 + c.numb * 10 + d.numb as numb
from numbers as a
cross join numbers as b
cross join numbers as c
cross join numbers as d
), full_dates as (
select
    date(c_date.extraction_date, '+' || series.numb || ' days') as extraction_date
from series
cross join (select current_date as extraction_date) c_date
)
select
    row_number() over(order by extraction_date asc) as extraction_id
    ,extraction_date
    ,null as uploaded
from full_dates;
