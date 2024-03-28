-- viz type=table, name=Compute Access Mode Limitation Summary, columns=issue,link,distinct_workspaces,distinct_notebooks,distinct_clusters,distinct_users
-- widget title=Compute Access Mode Limitation Summary, row=1, col=0, size_x=6, size_y=12
-- Scan notebook command history for potential paper cut issues
-- https://docs.databricks.com/en/compute/access-mode-limitations.html#compute-access-mode-limitations
-- 
WITH 
iteractive_cluster_commands (
    SELECT  
        a.event_id,
        a.request_params.notebookId AS notebook_id,
        a.request_params.clusterId AS cluster_id,
        a.workspace_id,
        a.user_identity.email,
        try_cast(split(c.spark_version,'[\.-]')[0] as INT) dbr_version_major,
        split(c.spark_version,'[\.-]')[1] dbr_version_minor,
        split(c.spark_version,'[\.-]')[3] dbr_type,
        a.request_params.commandLanguage,
        a.request_params.commandText,
        md5(a.request_params.commandText) commandHash
    FROM system.access.audit a
        LEFT OUTER JOIN $inventory.clusters AS c 
            ON a.request_params.clusterId = c.cluster_id
        AND a.action_name = 'runCommand'
    WHERE a.event_date >= DATE_SUB(CURRENT_DATE(), 90)
),
misc_patterns(
    SELECT commandLanguage, dbr_version_major, dbr_version_minor, dbr_type, pattern, issue FROM $inventory.misc_patterns
),
pattern_matcher(
    SELECT
        array_except(array(p.issue, lp.issue, rv.issue,dbr_type.issue), array(null)) issues,
        a.*
    FROM iteractive_cluster_commands a
        LEFT OUTER JOIN $inventory.code_patterns p 
            ON a.commandLanguage in ('python','scala')
                AND contains(a.commandText, p.pattern)
        LEFT OUTER JOIN misc_patterns lp                                                       
            ON a.commandLanguage = lp.commandLanguage
        LEFT OUTER JOIN misc_patterns rv -- runtime version                                                       
            ON (a.commandLanguage = rv.commandLanguage OR rv.commandLanguage is null)
                AND a.dbr_version_major < rv.dbr_version_major
                AND rv.dbr_version_major is not null
        LEFT OUTER JOIN misc_patterns dbr_type                                                         
            ON a.dbr_type = dbr_type.dbr_type and a.dbr_type in ('cpu','gpu')
),
exp (
    select distinct explode(issues) issue, workspace_id, notebook_id, cluster_id, email
    FROM pattern_matcher
)
SELECT 
    issue `Finding`,
    -- concat('<a href="https://github.com/databrickslabs/ucx/blob/main/docs/assessment.md#',replace(issue,' ','-'),'">',issue,'</a>') as link,
    count(distinct workspace_id) `# workspaces`,
    count(distinct notebook_id)  `# notebooks`,
    count(distinct cluster_id)   `# clusters`,
    count(distinct email)        `# users`
FROM exp
group by 1
order by 1