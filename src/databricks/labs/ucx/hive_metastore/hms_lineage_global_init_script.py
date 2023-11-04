global_init_script = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  driver_conf=${DB_HOME}/driver/conf/spark-branch.conf
  if [ ! -e $driver_conf ] ; then
    touch $driver_conf
  fi
cat << EOF >>  $driver_conf
  [driver] {
   "spark.databricks.dataLineage.enabled" = true
   }
EOF
fi"""
