from pathlib import Path

from s3up.domain.config import S3Config
from s3up.domain.object_key_strategy import ObjectKeyStrategy
from s3up.infrastructure.image_serializer import (
    image_tensor_to_bytes,
)
from s3up.infrastructure.retry_worker import get_retry_worker
from s3up.infrastructure.s3_client import S3ClientAdapter
from s3up.infrastructure.spool_repository import (
    SpoolRepository,
)
from s3up.infrastructure.upload_orchestrator import (
    UploadOrchestrator,
)


class S3UploadNode:
    """ComfyUI node that uploads images to S3."""

    @classmethod
    def INPUT_TYPES(cls):
        """Define ComfyUI input types."""
        base_dir = Path(__file__).resolve().parents[1]
        env = S3Config.env_defaults(base_dir)
        return {
            "required": {"images": ("IMAGE",)},
            "optional": {
                "endpoint": ("STRING", {"default": env["endpoint"]}),
                "bucket": ("STRING", {"default": env["bucket"]}),
                "region": ("STRING", {"default": env["region"]}),
                "access_key_id": ("STRING", {"default": ""}),
                "secret_access_key": ("STRING", {"default": ""}),
                "use_ssl": ("BOOLEAN", {"default": env["use_ssl"]}),
                "force_path_style": (
                    "BOOLEAN",
                    {"default": env["force_path_style"]},
                ),
                "prefix": ("STRING", {"default": env["prefix"]}),
                "spool_dir": (
                    "STRING",
                    {"default": str(env["spool_dir"])},
                ),
                "retry_max": ("INT", {"default": env["retry_max"]}),
                "retry_backoff_seconds": (
                    "INT",
                    {"default": env["retry_backoff_seconds"]},
                ),
                "retry_interval_seconds": (
                    "INT",
                    {"default": env["retry_interval_seconds"]},
                ),
                "retry_concurrency": (
                    "INT",
                    {"default": env["retry_concurrency"]},
                ),
            },
        }

    RETURN_TYPES = ()
    FUNCTION = "store"
    OUTPUT_NODE = True
    CATEGORY = "S3???"

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

