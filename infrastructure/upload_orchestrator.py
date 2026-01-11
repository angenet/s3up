import uuid
from dataclasses import dataclass

from ..domain.config import S3Config
from ..domain.object_key_strategy import ObjectKeyStrategy
from ..domain.spool_job import SpoolJob
from ..infrastructure.s3_client import S3ClientAdapter
from ..infrastructure.spool_repository import SpoolRepository


@dataclass(frozen=True)
class UploadOrchestrator:
    """Coordinate upload and spool fallback."""

    config: S3Config
    s3_client: S3ClientAdapter
    spool_repository: SpoolRepository
    key_strategy: ObjectKeyStrategy

    def upload_or_spool(self, image_bytes: bytes) -> None:
        """Upload bytes or spool if upload fails."""
        object_key = self.key_strategy.build_key(image_bytes)
        try:
            self.s3_client.upload_bytes(image_bytes, object_key)
        except Exception as exc:
            self._spool(image_bytes, object_key, str(exc))

    def _spool(self, image_bytes: bytes, object_key: str, error: str) -> None:
        job_id = uuid.uuid4().hex
        job = SpoolJob.create(
            job_id=job_id,
            object_key=object_key,
            bucket=self.config.bucket,
            endpoint=self.config.endpoint,
            file_path="",
        )
        updated = job.increment_retry(error)
        self.spool_repository.save_job(image_bytes, updated)

