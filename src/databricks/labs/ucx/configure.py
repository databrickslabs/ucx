import logging

from databricks.sdk.service import compute

logger = logging.getLogger(__name__)
CLUSTER_ID_LENGTH = 20  # number of characters in a valid cluster_id


class ConfigureMixin:
    """Installation configuration operations to suplement install.WorkspaceInstaller"""

    def _valid_cluster_id(self, cluster_id: str) -> bool:
        return cluster_id is not None and CLUSTER_ID_LENGTH == len(cluster_id)

    def _configure_override_clusters(self):
        """User may override standard job clusters with interactive clusters"""
        logger.info("Configuring cluster overrides from existing clusters")

        def is_classic(c) -> bool:
            return (
                c.state == compute.State.RUNNING
                and c.spark_version > "13.3.x"
                and c.data_security_mode == compute.DataSecurityMode.NONE
            )

        def is_tacl(c) -> bool:
            return (
                c.state == compute.State.RUNNING
                and c.spark_version > "13.3.x"
                and c.data_security_mode == compute.DataSecurityMode.LEGACY_TABLE_ACL
            )

        def build_and_prompt(prompt, clusters):
            choices = {"[Default, use a job cluster]": None}
            for _ in clusters:
                choices[_.cluster_name] = _.cluster_id
            return self._choice_from_dict(prompt, choices=choices)

        clusters = self._ws.clusters.list(can_use_client="NOTEBOOK")
        classic_clusters = [c for c in clusters if is_classic(c)]
        tacl_clusters = [c for c in clusters if is_tacl(c)]

        preamble = """We detected an install issue and recommend using existing clusters for the upgrade tasks ahead, please choose a """
        legacy_prompt = preamble + "pre-existing HMS Legacy cluster ID"
        tacl_prompt = preamble + "pre-existing Table Access Control cluster ID"

        cluster_id = build_and_prompt(legacy_prompt, classic_clusters)
        logger.info(f"classic cluster id {cluster_id} {self._valid_cluster_id(cluster_id)}")

        tacl_cluster_id = build_and_prompt(tacl_prompt, tacl_clusters)
        logger.info(f"tacl cluster choosen {tacl_cluster_id} {self._valid_cluster_id(tacl_cluster_id)}")

        overrides = None
        if self._valid_cluster_id(cluster_id) and self._valid_cluster_id(tacl_cluster_id):
            overrides = {
                "main": cluster_id,
                "tacl": tacl_cluster_id,
            }
            logger.info(f"Returning overrides {overrides}")
        return overrides
