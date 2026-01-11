from dataclasses import dataclass
from datetime import datetime, timezone
import uuid


@dataclass(frozen=True)
class ObjectKeyStrategy:
    """生成 S3 对象名称的策略。"""

    prefix: str
    use_timestamp_prefix: bool

    def build_key(
        self, extension: str, now: datetime | None = None
    ) -> str:
        """生成对象名称，不创建目录层级。"""
        current = now or datetime.now(timezone.utc)
        timestamp = current.strftime("%Y%m%d_%H%M%S_%f")
        random_hex = uuid.uuid4().hex[:8]
        safe_ext = extension.lstrip(".") or "bin"
        safe_prefix = self.prefix.strip("/")
        if self.use_timestamp_prefix:
            filename = f"{timestamp}_{random_hex}.{safe_ext}"
        else:
            filename = f"{random_hex}.{safe_ext}"
        if safe_prefix:
            return f"{safe_prefix}/{filename}"
        return filename

