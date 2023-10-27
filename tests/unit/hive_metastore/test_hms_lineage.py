from unittest.mock import Mock

from databricks.labs.ucx.hive_metastore.hms_lineage import HiveMetastoreLineageEnabler


def test_add_spark_config_for_hms_lineage(mocker):
    ws = mocker.Mock()
    _init_script_content = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
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
    ws.global_init_scripts.create.return_value = "abcde"

    hmle = HiveMetastoreLineageEnabler(ws)
    hmle.add_spark_config_for_hms_lineage()
