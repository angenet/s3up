import threading
import time
from dataclasses import dataclass, field

from custom_nodes.s3up.domain.config import S3Config
from custom_nodes.s3up.domain.spool_job import SpoolJob
from custom_nodes.s3up.infrastructure.s3_client import S3ClientAdapter
from custom_nodes.s3up.infrastructure.spool_repository import SpoolRepository


@dataclass
class RetryWorker:
    """Background worker to retry spooled uploads."""

    config: S3Config
    s3_client: S3ClientAdapter
    spool_repository: SpoolRepository
    _thread: threading.Thread | None = None
    _stop_event: threading.Event = field(default_factory=threading.Event)

    def start(self) -> None:
        """Start the background worker if not running."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="s3up-retry-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the background worker."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)

    def update(
        self,
        config: S3Config,
        s3_client: S3ClientAdapter,
        spool_repository: SpoolRepository,
    ) -> None:
        """Update worker dependencies for new configuration values."""
        self.config = config
        self.s3_client = s3_client
        self.spool_repository = spool_repository

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._process_once()
            time.sleep(self.config.retry_interval_seconds)

    def _process_once(self) -> None:
        job_paths = self.spool_repository.list_jobs()
        for job_path in job_paths:
            job = self.spool_repository.load_job(job_path)
            if job.retry_count >= self.config.retry_max:
                continue
            self._retry_job(job)

    def _retry_job(self, job: SpoolJob) -> None:
        if job.retry_count > 0:
            # Simple backoff to reduce repeated bursts.
            time.sleep(self.config.retry_backoff_seconds)
        try:
            self.s3_client.upload_file(job.file_path, job.object_key)
            self.spool_repository.delete_job(job)
        except Exception as exc:
            updated = job.increment_retry(str(exc))
            self.spool_repository.write_job(updated)


_worker_instance: RetryWorker | None = None
_worker_lock = threading.Lock()


def get_retry_worker(
    config: S3Config,
    s3_client: S3ClientAdapter,
    spool_repository: SpoolRepository,
) -> RetryWorker:
    """Return a singleton retry worker."""
    global _worker_instance
    with _worker_lock:
        if _worker_instance is None:
            _worker_instance = RetryWorker(
                config=config,
                s3_client=s3_client,
                spool_repository=spool_repository,
            )
        else:
            _worker_instance.update(
                config=config,
                s3_client=s3_client,
                spool_repository=spool_repository,
            )
        return _worker_instance

