/* --title 'Findings by Cluster' --width 6 */
WITH iteractive_cluster_commands AS (
  SELECT
    a.event_id,
    a.request_params.notebookId AS notebook_id,
    a.request_params.clusterId AS cluster_id,
    a.workspace_id,
    a.user_identity.email,
    TRY_CAST(SPLIT(c.spark_version, '[\\.-]')[0] AS INT) AS dbr_version_major,
    TRY_CAST(SPLIT(c.spark_version, '[\\.-]')[1] AS INT) AS dbr_version_minor,
    SPLIT(c.spark_version, '[\\.-]')[3] AS dbr_type,
    c.spark_version AS dbr_version,
    a.request_params.commandlanguage,
    a.request_params.commandtext,
    MD5(a.request_params.commandtext) AS commandhash,
    c.cluster_name,
    c.creator,
    a.event_date
  FROM system.access.audit AS a
  LEFT OUTER JOIN inventory.clusters AS c
    ON a.request_params.clusterId = c.cluster_id AND a.action_name = 'runCommand'
  WHERE
    a.event_date >= DATEADD(DAY, 90 * -1, CURRENT_DATE)
), misc_patterns AS (
  SELECT
    commandlanguage,
    dbr_version_major,
    dbr_version_minor,
    dbr_type,
    pattern,
    issue
  FROM inventory.misc_patterns
), pattern_matcher AS (
  SELECT
    EXPLODE(ARRAY_EXCEPT(ARRAY(p.issue, lp.issue, rv.issue, dbr_type.issue), ARRAY(NULL))) AS issue,
    a.*
  FROM iteractive_cluster_commands AS a
  LEFT OUTER JOIN inventory.code_patterns AS p
    ON a.commandlanguage IN ('python', 'scala') AND CONTAINS(a.commandtext, p.pattern)
  LEFT OUTER JOIN misc_patterns AS lp
    ON a.commandlanguage = lp.commandlanguage
  LEFT OUTER JOIN misc_patterns AS rv /* runtime version */
    ON (
      a.commandlanguage = rv.commandlanguage OR rv.commandlanguage IS NULL
    )
    AND a.dbr_version_major < rv.dbr_version_major
    AND NOT rv.dbr_version_major IS NULL
  LEFT OUTER JOIN misc_patterns AS dbr_type
    ON a.dbr_type = dbr_type.dbr_type AND a.dbr_type IN ('cpu', 'gpu')
)
SELECT
  COLLECT_LIST(DISTINCT issue) AS `Distinct Findings`,
  COUNT(1) AS `Commands`,
  COUNT(DISTINCT email) AS `Users`,
  CAST(MIN(event_date) AS STRING) AS `First command`,
  CAST(MAX(event_date) AS STRING) AS `Last command`,
  workspace_id,
  cluster_id,
  COALESCE(cluster_name, '<not in this workspace?>') AS cluster_name,
  COALESCE(LEFT(dbr_version, 30), '') AS dbr_version,
  COALESCE(creator, '') AS creator
FROM pattern_matcher
GROUP BY ALL
HAVING
  MAX(event_date) >= DATEADD(DAY, 15 * -1, CURRENT_DATE) /* active in last N days */
ORDER BY
  `Last command` DESC,
  `First command` ASC,
  COALESCE(cluster_name, cluster_id)