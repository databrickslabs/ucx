-- viz type=table, name=Failure Summary, columns=issue,component,issue_count,issue_percentage_jobs,issue_percentage_clusters,issue_percentage_gis,issue_percentage_pipelines
-- widget title = Failure Summary, col=4, row=0, size_x=5, size_y=3
WITH failuretab (failures, component) AS (
    SELECT
        failures,
        "jobs" AS component
    FROM
        $inventory.jobs
    UNION
    ALL
    SELECT
        failures,
        "clusters" AS component
    FROM
        $inventory.clusters
    UNION
    ALL
    SELECT
        failures,
        "global init scripts" AS component
    FROM
        $inventory.global_init_scripts
    UNION
    ALL
    SELECT
        failures,
        "pipelines" AS component
    FROM
        $inventory.pipelines
)
SELECT
    issue,
    component,
    COUNT(*) AS issue_count,
    IF (
        component = 'jobs',
        round(
            issue_count / (
                SELECT
                    count(*)
                FROM
                    $inventory.jobs
            ),
            2
        ) * 100,
        'NA'
    ) AS issue_percentage_jobs,
    IF (
        component = 'clusters',
        round(
            issue_count / (
                SELECT
                    count(*)
                FROM
                    $inventory.clusters
            ),
            2
        ) * 100,
        'NA'
    ) AS issue_percentage_clusters,
    IF (
        component = 'global init scripts',
        round(
            issue_count / (
                SELECT
                    count(*)
                FROM
                    $inventory.global_init_scripts
            ),
            2
        ) * 100,
        'NA'
    ) AS issue_percentage_gis,
    IF (
        component = 'pipelines',
        round(
            issue_count / (
                SELECT
                    count(*)
                FROM
                    $inventory.pipelines
            ),
            2
        ) * 100,
        'NA'
    ) AS issue_percentage_pipelines
FROM
    (
        SELECT
            explode(from_json(failures, 'array<string>')) AS failure,
            substring_index(failure, ":", 1) issue,
            component,
            IF (
                locate("not supported DBR:", failure) > 0,
                TRUE,
                FALSE
            ) AS incomp_dbr_present_or_not,
            IF (
                locate("unsupported config:", failure) > 0,
                TRUE,
                FALSE
            ) AS unsup_config_present_or_not,
            IF (
                locate("using DBFS mount in configuration:", failure) > 0,
                TRUE,
                FALSE
            ) AS dbfs_mount_present_or_not,
            IF (
                locate(
                    "Uses azure service principal credentials config in",
                    failure
                ) > 0,
                TRUE,
                FALSE
            ) AS azure_spn_present_or_not
        FROM
            failuretab
    )
WHERE
    (
        incomp_dbr_present_or_not IS TRUE
        OR unsup_config_present_or_not IS TRUE
        OR dbfs_mount_present_or_not IS TRUE
        OR azure_spn_present_or_not IS TRUE
    )
GROUP BY
    issue,
    component
ORDER BY
    issue,
    component