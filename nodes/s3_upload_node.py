from pathlib import Path

from ..domain.config import S3Config
from ..domain.object_key_strategy import ObjectKeyStrategy
from ..infrastructure.image_serializer import (
    image_tensor_to_bytes,
)
from ..infrastructure.retry_worker import get_retry_worker
from ..infrastructure.s3_client import S3ClientAdapter
from ..infrastructure.spool_repository import (
    SpoolRepository,
)
from ..infrastructure.upload_orchestrator import (
    UploadOrchestrator,
)


def _opt(input_type: str, default, label: str) -> tuple:
    options = {"label": label}
    if default is not None:
        options["default"] = default
    return (input_type, options)


class S3UploadNode:
    """ComfyUI node that uploads images to S3."""

    @classmethod
    def INPUT_TYPES(cls):
        """Define ComfyUI input types."""
        base_dir = Path(__file__).resolve().parents[1]
        env = S3Config.env_defaults(base_dir)
        return {
            "required": {"images": _opt("IMAGE", None, "图片")},
            "optional": {
                "endpoint": _opt("STRING", env["endpoint"], "端点"),
                "bucket": _opt("STRING", env["bucket"], "存储桶"),
                "region": _opt("STRING", env["region"], "区域"),
                "access_key_id": _opt("STRING", "", "访问密钥"),
                "secret_access_key": _opt(
                    "STRING", "", "访问密钥密文"
                ),
                "use_ssl": _opt("BOOLEAN", env["use_ssl"], "启用SSL"),
                "force_path_style": _opt(
                    "BOOLEAN", env["force_path_style"], "路径风格"
                ),
                "prefix": _opt("STRING", env["prefix"], "对象前缀"),
                "spool_dir": _opt(
                    "STRING", str(env["spool_dir"]), "落盘目录"
                ),
                "retry_max": _opt("INT", env["retry_max"], "最大重试"),
                "retry_backoff_seconds": _opt(
                    "INT", env["retry_backoff_seconds"], "退避秒数"
                ),
                "retry_interval_seconds": _opt(
                    "INT", env["retry_interval_seconds"], "扫描间隔"
                ),
                "retry_concurrency": _opt(
                    "INT", env["retry_concurrency"], "补传并发"
                ),
            },
        }

    RETURN_TYPES = ()
    FUNCTION = "store"
    OUTPUT_NODE = True
    CATEGORY = "S3存储"

    def __init__(self):
        self._base_dir = Path(__file__).resolve().parents[1]

    def store(
        self,
        images,
        endpoint="",
        bucket="",
        region="",
        access_key_id="",
        secret_access_key="",
        use_ssl=None,
        force_path_style=None,
        prefix="",
        spool_dir="",
        retry_max=None,
        retry_backoff_seconds=None,
        retry_interval_seconds=None,
        retry_concurrency=None,
    ):
        """Store images to S3 or spool on failure."""
        overrides = {
            "endpoint": endpoint,
            "bucket": bucket,
            "region": region,
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
            "use_ssl": use_ssl,
            "force_path_style": force_path_style,
            "prefix": prefix,
            "spool_dir": spool_dir,
            "retry_max": retry_max,
            "retry_backoff_seconds": retry_backoff_seconds,
            "retry_interval_seconds": retry_interval_seconds,
            "retry_concurrency": retry_concurrency,
        }
        config = S3Config.from_sources(self._base_dir, overrides)
        s3_client = S3ClientAdapter(config=config)
        spool_repository = SpoolRepository(base_dir=config.spool_dir)
        key_strategy = ObjectKeyStrategy(prefix=config.prefix)
        orchestrator = UploadOrchestrator(
            config=config,
            s3_client=s3_client,
            spool_repository=spool_repository,
            key_strategy=key_strategy,
        )
        worker = get_retry_worker(
            config=config,
            s3_client=s3_client,
            spool_repository=spool_repository,
        )
        worker.start()
        image_bytes = image_tensor_to_bytes(images)
        orchestrator.upload_or_spool(image_bytes)
        return ()

