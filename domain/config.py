import os
from dataclasses import dataclass
from pathlib import Path

from ..domain.exceptions import DomainException


@dataclass(frozen=True)
class S3Config:
    """S3 上传与失败补传的配置。"""

    endpoint: str
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str
    use_ssl: bool
    force_path_style: bool
    prefix: str
    use_timestamp_prefix: bool
    spool_dir: Path
    retry_max: int
    retry_backoff_seconds: int
    retry_interval_seconds: int
    retry_concurrency: int

    @classmethod
    def from_env(cls, base_dir: Path) -> "S3Config":
        """从环境变量读取配置。"""
        env = cls.env_defaults(base_dir)
        endpoint = env["endpoint"]
        bucket = env["bucket"]
        region = env["region"]
        access_key_id = env["access_key_id"]
        secret_access_key = env["secret_access_key"]
        prefix = env["prefix"]
        use_timestamp_prefix = env["use_timestamp_prefix"]
        use_ssl = env["use_ssl"]
        force_path_style = env["force_path_style"]
        spool_dir = env["spool_dir"]
        retry_max = env["retry_max"]
        retry_backoff_seconds = env["retry_backoff_seconds"]
        retry_interval_seconds = env["retry_interval_seconds"]
        retry_concurrency = env["retry_concurrency"]
        config = cls(
            endpoint=endpoint,
            bucket=bucket,
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            use_ssl=use_ssl,
            force_path_style=force_path_style,
            prefix=prefix,
            use_timestamp_prefix=use_timestamp_prefix,
            spool_dir=spool_dir,
            retry_max=retry_max,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_interval_seconds=retry_interval_seconds,
            retry_concurrency=retry_concurrency,
        )
        config._validate()
        return config

    @classmethod
    def from_sources(
        cls, base_dir: Path, overrides: dict
    ) -> "S3Config":
        """读取环境变量，并用输入参数覆盖。"""
        env = cls.env_defaults(base_dir)
        config = cls(
            endpoint=_pick_str(overrides.get("endpoint"), env["endpoint"]),
            bucket=_pick_str(overrides.get("bucket"), env["bucket"]),
            region=_pick_str(overrides.get("region"), env["region"]),
            access_key_id=_pick_str(
                overrides.get("access_key_id"), env["access_key_id"]
            ),
            secret_access_key=_pick_str(
                overrides.get("secret_access_key"),
                env["secret_access_key"],
            ),
            use_ssl=_pick_bool(overrides.get("use_ssl"), env["use_ssl"]),
            force_path_style=_pick_bool(
                overrides.get("force_path_style"),
                env["force_path_style"],
            ),
            prefix=_pick_str(overrides.get("prefix"), env["prefix"]),
            use_timestamp_prefix=_pick_bool(
                overrides.get("use_timestamp_prefix"),
                env["use_timestamp_prefix"],
            ),
            spool_dir=_pick_path(
                overrides.get("spool_dir"), env["spool_dir"]
            ),
            retry_max=_pick_int(overrides.get("retry_max"), env["retry_max"]),
            retry_backoff_seconds=_pick_int(
                overrides.get("retry_backoff_seconds"),
                env["retry_backoff_seconds"],
            ),
            retry_interval_seconds=_pick_int(
                overrides.get("retry_interval_seconds"),
                env["retry_interval_seconds"],
            ),
            retry_concurrency=_pick_int(
                overrides.get("retry_concurrency"),
                env["retry_concurrency"],
            ),
        )
        config._validate()
        return config

    @classmethod
    def env_defaults(cls, base_dir: Path) -> dict:
        """读取环境变量默认值，不做必填检查。"""
        endpoint = os.getenv("S3_ENDPOINT", "").strip()
        bucket = os.getenv("S3_BUCKET", "").strip()
        region = os.getenv("S3_REGION", "us-east-1").strip()
        access_key_id = os.getenv("S3_ACCESS_KEY_ID", "").strip()
        secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY", "").strip()
        prefix = os.getenv("S3_PREFIX", "").strip()
        use_timestamp_prefix = _parse_bool_default(
            os.getenv("S3_TIMESTAMP_PREFIX", "true"), True
        )
        use_ssl = _parse_bool_default(
            os.getenv("S3_USE_SSL", "true"), True
        )
        force_path_style = _parse_bool_default(
            os.getenv("S3_FORCE_PATH_STYLE", "false"), False
        )
        spool_dir = Path(
            os.getenv("S3_SPOOL_DIR", str(base_dir / "spool"))
        )
        retry_max = _parse_int_default(os.getenv("S3_RETRY_MAX", "5"), 5)
        retry_backoff_seconds = _parse_int_default(
            os.getenv("S3_RETRY_BACKOFF_SECONDS", "2"), 2
        )
        retry_interval_seconds = _parse_int_default(
            os.getenv("S3_RETRY_INTERVAL_SECONDS", "5"), 5
        )
        retry_concurrency = _parse_int_default(
            os.getenv("S3_RETRY_CONCURRENCY", "1"), 1
        )
        return {
            "endpoint": endpoint,
            "bucket": bucket,
            "region": region,
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
            "use_ssl": use_ssl,
            "force_path_style": force_path_style,
            "prefix": prefix,
            "use_timestamp_prefix": use_timestamp_prefix,
            "spool_dir": spool_dir,
            "retry_max": retry_max,
            "retry_backoff_seconds": retry_backoff_seconds,
            "retry_interval_seconds": retry_interval_seconds,
            "retry_concurrency": retry_concurrency,
        }

    def _validate(self) -> None:
        """检查必填配置。"""
        if not self.bucket:
            raise DomainException("S3_BUCKET 必须填写")
        if not self.access_key_id:
            raise DomainException("S3_ACCESS_KEY_ID 必须填写")
        if not self.secret_access_key:
            raise DomainException("S3_SECRET_ACCESS_KEY 必须填写")


def _parse_bool(value: str) -> bool:
    """把布尔字符串转为布尔值。"""
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _parse_int(value: str) -> int:
    """把整数文本转为整数。"""
    try:
        return int(value)
    except ValueError as exc:
        raise DomainException("整数格式不正确") from exc


def _parse_int_default(value: str, fallback: int) -> int:
    """解析整数，失败时使用默认值。"""
    try:
        return int(value)
    except ValueError:
        return fallback


def _parse_bool_default(value: str, fallback: bool) -> bool:
    """解析布尔值，失败时使用默认值。"""
    text = value.strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return fallback


def _pick_str(value: str | None, fallback: str) -> str:
    """读取字符串，空值就用默认值。"""
    if value is None:
        return fallback
    if isinstance(value, str) and value.strip() == "":
        return fallback
    return str(value).strip()


def _pick_bool(value: bool | None, fallback: bool) -> bool:
    """读取布尔值，空值就用默认值。"""
    if value is None:
        return fallback
    return bool(value)


def _pick_int(value: int | None, fallback: int) -> int:
    """读取整数值，空值就用默认值。"""
    if value is None:
        return fallback
    return int(value)


def _pick_path(value: str | None, fallback: Path) -> Path:
    """读取路径，空值就用默认值。"""
    if value is None:
        return fallback
    if isinstance(value, str) and value.strip() == "":
        return fallback
    return Path(str(value))

