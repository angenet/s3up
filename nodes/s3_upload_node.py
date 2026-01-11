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


def _opt(input_type: str, default, label: str, tooltip: str) -> tuple:
    options = {"label": label, "tooltip": tooltip}
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
            "required": {
                "images": _opt(
                    "IMAGE", None, "图像输入", "需要保存的图像"
                )
            },
            "optional": {
                "endpoint": _opt(
                    "STRING", env["endpoint"], "服务地址", "S3兼容服务地址"
                ),
                "bucket": _opt(
                    "STRING", env["bucket"], "桶名称", "目标桶名称"
                ),
                "region": _opt(
                    "STRING", env["region"], "区域", "区域代码"
                ),
                "access_key_id": _opt(
                    "STRING", "", "访问密钥", "账号 Access Key"
                ),
                "secret_access_key": _opt(
                    "STRING", "", "访问密钥密文", "账号 Secret Key"
                ),
                "use_ssl": _opt(
                    "BOOLEAN", env["use_ssl"], "启用HTTPS", "是否启用HTTPS"
                ),
                "force_path_style": _opt(
                    "BOOLEAN",
                    env["force_path_style"],
                    "路径风格",
                    "兼容MinIO时可开启",
                ),
                "prefix": _opt("STRING", env["prefix"], "对象前缀", "保存目录"),
                "use_timestamp_prefix": _opt(
                    "BOOLEAN",
                    env["use_timestamp_prefix"],
                    "时间戳前缀",
                    "文件名以时间戳开头",
                ),
                "spool_dir": _opt(
                    "STRING",
                    str(env["spool_dir"]),
                    "失败暂存目录",
                    "失败时保存到本地的目录",
                ),
                "retry_max": _opt(
                    "INT", env["retry_max"], "最大重试次数", "最大补传次数"
                ),
                "retry_backoff_seconds": _opt(
                    "INT",
                    env["retry_backoff_seconds"],
                    "退避等待秒数",
                    "连续失败后的等待时间",
                ),
                "retry_interval_seconds": _opt(
                    "INT",
                    env["retry_interval_seconds"],
                    "扫描间隔秒数",
                    "后台补传扫描间隔",
                ),
                "retry_concurrency": _opt(
                    "INT",
                    env["retry_concurrency"],
                    "补传并发",
                    "同时补传的任务数量",
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
        use_timestamp_prefix=None,
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
            "use_timestamp_prefix": use_timestamp_prefix,
            "spool_dir": spool_dir,
            "retry_max": retry_max,
            "retry_backoff_seconds": retry_backoff_seconds,
            "retry_interval_seconds": retry_interval_seconds,
            "retry_concurrency": retry_concurrency,
        }
        config = S3Config.from_sources(self._base_dir, overrides)
        s3_client = S3ClientAdapter(config=config)
        spool_repository = SpoolRepository(base_dir=config.spool_dir)
        key_strategy = ObjectKeyStrategy(
            prefix=config.prefix,
            use_timestamp_prefix=config.use_timestamp_prefix,
        )
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

