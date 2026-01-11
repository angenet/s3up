import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from s3up.domain.spool_job import SpoolJob


@dataclass(frozen=True)
class SpoolRepository:
    """Persist and load spool jobs and files."""

    base_dir: Path

    def save_job(self, image_bytes: bytes, job: SpoolJob) -> SpoolJob:
        """Persist image bytes and job metadata to disk."""
        self._ensure_dirs()
        file_id = uuid.uuid4().hex
        file_path = self._files_dir() / f"{file_id}.png"
        job_path = self._jobs_dir() / f"{job.job_id}.json"
        file_path.write_bytes(image_bytes)
        updated = SpoolJob(
            job_id=job.job_id,
            object_key=job.object_key,
            bucket=job.bucket,
            endpoint=job.endpoint,
            file_path=str(file_path),
            retry_count=job.retry_count,
            last_error=job.last_error,
            created_at=job.created_at,
        )
        job_path.write_text(json.dumps(updated.to_dict()), encoding="utf-8")
        return updated

    def list_jobs(self) -> Iterable[Path]:
        """Return job file paths currently in the spool."""
        if not self._jobs_dir().exists():
            return []
        return list(self._jobs_dir().glob("*.json"))

    def load_job(self, job_path: Path) -> SpoolJob:
        """Load a job from a JSON file."""
        payload = json.loads(job_path.read_text(encoding="utf-8"))
        return SpoolJob.from_dict(payload)

    def write_job(self, job: SpoolJob) -> None:
        """Update a job JSON file in the spool."""
        job_path = self._jobs_dir() / f"{job.job_id}.json"
        job_path.write_text(json.dumps(job.to_dict()), encoding="utf-8")

    def delete_job(self, job: SpoolJob) -> None:
        """Remove job and associated file from disk."""
        job_path = self._jobs_dir() / f"{job.job_id}.json"
        file_path = Path(job.file_path)
        if job_path.exists():
            job_path.unlink()
        if file_path.exists():
            file_path.unlink()

    def _ensure_dirs(self) -> None:
        self._jobs_dir().mkdir(parents=True, exist_ok=True)
        self._files_dir().mkdir(parents=True, exist_ok=True)

    def _jobs_dir(self) -> Path:
        return self.base_dir / "jobs"

    def _files_dir(self) -> Path:
        return self.base_dir / "files"

