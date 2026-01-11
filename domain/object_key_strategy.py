import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class ObjectKeyStrategy:
    """Build object keys for S3 uploads."""

    prefix: str
    use_timestamp_prefix: bool

    def build_key(self, content: bytes, now: datetime | None = None) -> str:
        """Build a stable object key from content and current time."""
        digest = hashlib.sha256(content).hexdigest()
        current = now or datetime.now(timezone.utc)
        date_path = current.strftime("%Y/%m/%d")
        timestamp = current.strftime("%Y%m%d%H%M%S")
        safe_prefix = self.prefix.strip("/")
        filename = f"{digest}.png"
        if self.use_timestamp_prefix:
            filename = f"{timestamp}_{filename}"
        if safe_prefix:
            return f"{safe_prefix}/{date_path}/{filename}"
        return f"{date_path}/{filename}"

