-- viz type=table, name=Interactive Cluster Assessment, columns=findings,distinct_notebooks, distinct_clusters, distinct_users,rank
-- widget title=Interactive, row=7, col=0, size_x=2, size_y=8

-- Scan notebook command history for potential paper cut issues
-- https://docs.databricks.com/en/compute/access-mode-limitations.html#compute-access-mode-limitations
-- This query 'overcounts' the paper cuts that might be encountered. There are complex DBR interactions with DBR 11, 12, 13 and 14
with paper_cut_patterns(
select col1 as pattern, col2 as issue from values
    ('hive_metastore.', 		'AF300 - 3 level namespace'),
    ('spark.catalog.', 			'AF301.1 - spark.catalog.x'),
    ('spark._jsparkSession.catalog',	'AF301.2 - spark.catalog.x'),
    ('spark._jspark', 			'AF302.1 - Spark Context'),
    ('spark._jvm',			'AF302.2 - Spark Context'),
    ('._jdf', 				'AF302.3 - Spark Context'),
    ('._jcol',				'AF302.4 - Spark Context'),
    ('spark.read.format("jdbc")', 	'AF304 - JDBC datasource'),
    ('dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()','AF305.1 - getContext'),
    ('dbutils.notebook.entry_point.getDbutils().notebook().getContext()','AF305.2 - getContext'),
    ('spark.udf.registerJavaFunction',	'AF306 - Java UDF'),
    ('boto3', 				'AF307.1 - boto3'),
    ('s3fs', 				'AF307.2 - s3fs'),
    ('from graphframes', 		'AF308 - Graphframes'),
    ('pyspark.ml.', 			'AF309 - Spark ML'),
    ('applyInPandas', 			'AF310.1 - applyInPandas'),
    ('mapInPandas', 			'AF310.2 - mapInPandas'),
    ('dbutils.fs.', 			'AF311 - dbutils.fs'),
    ('dbutils.credentials.', 		'AF312 - credential passthrough'), -- credential passthrough,
    ('dbfs:/mnt', 			'AF313.1 - mount points'),
    ('dbfs:/', 			'AF313 - dbfs usage'),
    ('sparknlp',     			'AF314 - Spark NLP John Snow Labs'),
    ('xgboost.spark', 'AF315 - XGBoost Spark'),
    ('catboost_spark', 'AF316 - CatBoost Spark'),
    ('ai.catboost:catboost-spark', 'AF316 - CatBoost Spark'),
    ('hyperopt', 'AF317 - Hyperopt'),
    ('SparkTrials', 'AF317 - Hyperopt'),
    ('horovod.spark', 'AF318 - Horovod'),
    ('UserDefinedAggregateFunction', 'AF319 - UDAF scala issue'),
    ('applyInPandasWithState', 'AF321 - streaming applyInPandasWithState'),
    ('ray.util.spark', 'AF322 - Apache Ray'),
    ('databricks.automl', 'AF323 - Databricks Auto ML')
),
sparkcontext (
    select explode(split(".rdd, _jvm, _jvm.org.apache.log4j, emptyRDD, range, init_batched_serializer, parallelize, pickleFile, textFile, wholeTextFiles, binaryFiles, binaryRecords, sequenceFile, newAPIHadoopFile, newAPIHadoopRDD, hadoopFile, hadoopRDD, union, runJob, setSystemProperty, uiWebUrl, stop, setJobGroup, setLocalProperty, getConf",', ')) as pattern,
    					'AF303.1 - RDD' as issue
    UNION ALL
    select explode(split("from pyspark.sql import SQLContext, import org.apache.spark.sql.SQLContext, spark.sparkContext ", ', ')) as pattern,				'AF303.2 - SQLContext' as issue
),
streaming (
    select explode(split('.trigger(continuous, kafka.sasl.client.callback.handler.class, kafka.sasl.login.callback.handler.class, kafka.sasl.login.class, kafka.partition.assignment.strategy, kafka.ssl.truststore.location, kafka.ssl.keystore.location, cleanSource, sourceArchiveDir, applyInPandasWithState, .format("socket"), StreamingQueryListener',', ')) pattern,
    					'AF330 - Streaming' as issue
),
paper_cuts(
    select pattern, issue from paper_cut_patterns
    UNION ALL
    select concat('sc.',pattern) as pattern, issue from sparkcontext
    UNION ALL
    select pattern, issue from streaming
),
iteractive_cluster_commands (
    select 
        a.request_params.notebookId as notebook_id, 
        a.request_params.clusterId as cluster_id, 
        a.user_identity.email,
        a.request_params.commandLanguage,
        a.request_params.commandText 
    from system.access.audit a
    join system.compute.clusters as c
        ON c.cluster_source != 'JOB'
        AND (c.tags.ResourceClass is null OR c.tags.ResourceClass != "SingleNode")
        AND a.action_name = 'runCommand' 
        AND a.request_params.clusterId = c.cluster_id
    WHERE
        a.event_date >= DATE_SUB(CURRENT_DATE(), 90)
),
python_matcher(
    select 
    p.issue, 
    a.notebook_id, 
    a.cluster_id, 
    a.email,
    a.commandLanguage,
    a.commandText 
from iteractive_cluster_commands a
join paper_cuts p
    ON a.commandLanguage = 'python'
    AND contains(a.commandText, p.pattern)
),
scala_matcher(
    select
        'AF320 - scala/R' as issue,
        a.notebook_id, 
        a.cluster_id, 
        a.email, 
        a.commandText  
    from iteractive_cluster_commands a
    where a.commandLanguage in ('scala','r')
),
unions(
    SELECT issue, notebook_id, cluster_id, email, commandText from python_matcher
    UNION ALL
    SELECT issue, notebook_id, cluster_id, email, commandText from scala_matcher
)
SELECT issue, 
    count(distinct notebook_id) distinct_notebooks,
    count(distinct cluster_id) distinct_clusters,
    count(distinct email) distinct_users
FROM unions group by 1
order by 1
;
