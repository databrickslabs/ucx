-- viz type=table, name=Findings by Cluster, columns=distinct_findings,Commands,Users,First_command,Last_command,workspace_id,cluster_id,cluster_name,dbr_version,creator
-- widget title=Findings by Cluster, row=3, col=0, size_x=6, size_y=12
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
        try_cast(split(c.spark_version,'[\.-]')[1] as INT) dbr_version_minor,
        split(c.spark_version,'[\.-]')[3] dbr_type,
        c.spark_version dbr_version,
        a.request_params.commandLanguage,
        a.request_params.commandText,
        md5(a.request_params.commandText) commandHash,
        c.cluster_name,
        c.creator,
        a.event_date
    FROM system.access.audit a
        LEFT OUTER JOIN $inventory.clusters AS c ON a.request_params.clusterId = c.cluster_id
        AND a.action_name = 'runCommand'
    WHERE a.event_date >= DATE_SUB(CURRENT_DATE(), 90)
),
misc_patterns(
    SELECT commandLanguage, dbr_version_major, dbr_version_minor, dbr_type, pattern, issue FROM $inventory.misc_patterns
),
pattern_matcher(
    SELECT
        explode(array_except(array(p.issue, lp.issue, rv.issue,dbr_type.issue), array(null))) issue,
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
)
SELECT
    collect_list(distinct issue) `Distinct Findings`,
    count(1) `Commands`,
    count(distinct email) `Users`,
    cast(min(event_date) as STRING) `First command`,
    cast(max(event_date) as STRING) `Last command`,
    workspace_id,
    cluster_id,
    coalesce(cluster_name,'<not in this workspace?>') cluster_name,
    coalesce(left(dbr_version,30),'') dbr_version,
    coalesce(creator,'') creator
FROM pattern_matcher
GROUP BY ALL
HAVING max(event_date) >= DATE_SUB(CURRENT_DATE(), 15) -- active in last N days
ORDER BY `Last command` desc, `First command` asc, coalesce(cluster_name,cluster_id)