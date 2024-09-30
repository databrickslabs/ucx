import base64
import logging
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


class SecretsMixin:
    _ws: WorkspaceClient

    SECRET_PATTERN = r"{{secrets\/(.*)\/(.*)}}"
    TENANT_PATTERN = r"https:\/\/login\.(?:partner\.)?microsoftonline\.(?:com|us|cn)\/(.*)\/oauth2\/token"

    def _get_secret_if_exists(self, secret_scope, secret_key) -> str | None:
        """Get the secret value given a secret scope & secret key. Log a warning if secret does not exist"""
        try:
            # Return the decoded secret value in string format
            secret = self._ws.secrets.get_secret(secret_scope, secret_key)
            assert secret.value is not None
            return base64.b64decode(secret.value).decode("utf-8")
        except NotFound:
            logger.warning(f'removed on the backend: {secret_scope}/{secret_key}')
            return None
        except UnicodeDecodeError:
            logger.warning(
                f"Secret {secret_scope}/{secret_key} has Base64 bytes that cannot be decoded to utf-8 string."
            )
            return None

    def _get_value_from_config_key(
        self,
        config: dict,
        key: str,
        get_secret: bool = True,
    ) -> str | None:
        """Get a config value based on its key, with some special handling:
        If the key is prefixed with spark_conf, i.e. this is in a cluster policy, the actual value is nested
        If the value is of format {{secret_scope/secret}}, we extract that as well
        """
        if re.search("spark_conf", key):
            value = config.get(key, {})
            if isinstance(value, dict):
                value = value.get("value", "")
        else:
            value = config.get(key, "")
        # retrieve from secret scope if used
        if not get_secret:
            return value
        secret_matched = re.findall(self.SECRET_PATTERN, value)
        if len(secret_matched) == 0:
            return value
        secret_scope, secret_key = secret_matched[0][0], secret_matched[0][1]
        return self._get_secret_if_exists(secret_scope, secret_key)

    def _get_client_secret(self, config: dict, secret_key: str) -> tuple[str | None, str | None]:
        client_secret = self._get_value_from_config_key(config, secret_key, False)
        if client_secret is None:
            return None, None
        secret_matched = re.findall(self.SECRET_PATTERN, client_secret)
        if len(secret_matched) == 0:
            logger.warning('Secret in config stored in plaintext.')
            return None, None
        return secret_matched[0][0], secret_matched[0][1]

    def _get_tenant_id(self, config: dict, tenant_key: str) -> str | None:
        tenant_value = self._get_value_from_config_key(config, tenant_key)
        if tenant_value is None:
            return None
        tenant_matched = re.findall(self.TENANT_PATTERN, tenant_value)
        if len(tenant_matched) == 0:
            logger.warning('Tenant id configuration is not in correct format')
            return None
        return tenant_matched[0]
