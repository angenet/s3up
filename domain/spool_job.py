from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class SpoolJob:
    """Represents a persisted upload retry job."""

    job_id: str
    object_key: str
    bucket: str
    endpoint: str
    file_path: str
    retry_count: int
    last_error: str
    created_at: str

    @classmethod
    def create(
        cls,
        job_id: str,
        object_key: str,
        bucket: str,
        endpoint: str,
        file_path: str,
    ) -> "SpoolJob":
        """Create a new job with default retry values."""
        created_at = datetime.now(timezone.utc).isoformat()
        return cls(
            job_id=job_id,
            object_key=object_key,
            bucket=bucket,
            endpoint=endpoint,
            file_path=file_path,
            retry_count=0,
            last_error="",
            created_at=created_at,
        )

    def to_dict(self) -> dict:
        """Serialize the job to a JSON-serializable dict."""
        return {
            "job_id": self.job_id,
            "object_key": self.object_key,
            "bucket": self.bucket,
            "endpoint": self.endpoint,
            "file_path": self.file_path,
            "retry_count": self.retry_count,
            "last_error": self.last_error,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "SpoolJob":
        """Load a job from a dict."""
        return cls(
            job_id=payload["job_id"],
            object_key=payload["object_key"],
            bucket=payload["bucket"],
            endpoint=payload["endpoint"],
            file_path=payload["file_path"],
            retry_count=payload["retry_count"],
            last_error=payload["last_error"],
            created_at=payload["created_at"],
        )

    def increment_retry(self, error: str) -> "SpoolJob":
        """Return a new job with incremented retry count."""
        return SpoolJob(
            job_id=self.job_id,
            object_key=self.object_key,
            bucket=self.bucket,
            endpoint=self.endpoint,
            file_path=self.file_path,
            retry_count=self.retry_count + 1,
            last_error=error,
            created_at=self.created_at,
        )

