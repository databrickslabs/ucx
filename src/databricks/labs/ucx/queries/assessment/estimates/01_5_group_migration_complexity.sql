-- viz type=counter, name=Group migration complexity, counter_label=Group migration complexity, value_column=group_migration_complexity
-- widget row=1, col=5, size_x=1, size_y=8
select
case when total_groups = 0 then NULL
when total_groups between 1 and 50 then "S"
when total_groups between 51 and 200 then "M"
when total_groups > 201 then "L"
ELSE NULL
end as group_migration_complexity from
(SELECT count(*) as total_groups FROM $inventory.groups)
