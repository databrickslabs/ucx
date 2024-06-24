-- --title 'Data modeling complexity' --height 10 --width 1
select
case when distinct_tables = 0 then NULL
when distinct_tables between 1 and 100 then "S"
when distinct_tables between 101 and 300 then "M"
when distinct_tables > 301 then "L"
else NULL end as uc_model_complexity from
(select count(distinct concat(database,".",name)) as distinct_tables from inventory.tables);
