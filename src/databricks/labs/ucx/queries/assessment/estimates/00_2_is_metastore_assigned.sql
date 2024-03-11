-- viz type=counter, name=Metastore assigned, value_column=uc_metastore_assigned
-- widget row=0, col=5, size_x=1, size_y=8
SELECT case when CURRENT_METASTORE() is not null then "Metastore already assigned" else "Metastore not assigned" end as uc_metastore_assigned
