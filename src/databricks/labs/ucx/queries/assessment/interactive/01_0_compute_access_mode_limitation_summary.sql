/* --title 'Compute Access Mode Limitation Summary' --width 6 */ /* Scan notebook command history for potential paper cut issues */ /* https://docs.databricks.com/en/compute/access-mode-limitations.html#compute-access-mode-limitations */
WITH iteractive_cluster_commands AS (
  SELECT
    a.event_id,
    a.request_params.notebookId AS notebook_id,
    a.request_params.clusterId AS cluster_id,
    a.workspace_id,
    a.user_identity.email,
    TRY_CAST(SPLIT(c.spark_version, '[\\.-]')[0] AS INT) AS dbr_version_major,
    SPLIT(c.spark_version, '[\\.-]')[1] AS dbr_version_minor,
    SPLIT(c.spark_version, '[\\.-]')[3] AS dbr_type,
    a.request_params.commandlanguage,
    a.request_params.commandtext,
    MD5(a.request_params.commandtext) AS commandhash
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
    ARRAY_EXCEPT(ARRAY(p.issue, lp.issue, rv.issue, dbr_type.issue), ARRAY(NULL)) AS issues,
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
), exp AS (
  SELECT DISTINCT
    EXPLODE(issues) AS issue,
    workspace_id,
    notebook_id,
    cluster_id,
    email
  FROM pattern_matcher
)
SELECT
  issue AS `Finding`,
  COUNT(DISTINCT workspace_id) AS `# workspaces`, /* concat('<a href="https://github.com/databrickslabs/ucx/blob/main/docs/assessment.md#',replace(issue,' ','-'),'">',issue,'</a>') as link, */
  COUNT(DISTINCT notebook_id) AS `# notebooks`,
  COUNT(DISTINCT cluster_id) AS `# clusters`,
  COUNT(DISTINCT email) AS `# users`
FROM exp
GROUP BY
  1
ORDER BY
  1