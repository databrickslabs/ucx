-- viz type=counter, name=Data modeling complexity, counter_label=Data modeling complexity, value_column=uc_model_complexity
-- widget row=2, col=5, size_x=1, size_y=8
select
case when distinct_tables = 0 then NULL
when distinct_tables between 1 and 100 then "S"
when distinct_tables between 101 and 300 then "M"
when distinct_tables > 301 then "L"
else NULL end as uc_model_complexity from
(select count(distinct concat(database,".",name)) as distinct_tables from $inventory.tables);