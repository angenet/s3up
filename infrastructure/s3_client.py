import boto3
from dataclasses import dataclass

from s3up.domain.config import S3Config


@dataclass(frozen=True)
class S3ClientAdapter:
    """S3 client adapter using boto3."""

    config: S3Config

    def upload_bytes(self, content: bytes, object_key: str) -> str:
        """Upload bytes and return ETag."""
        response = self._client().put_object(
            Bucket=self.config.bucket,
            Key=object_key,
            Body=content,
        )
        return response.get("ETag", "")

    def upload_file(self, file_path: str, object_key: str) -> str:
        """Upload file path and return ETag."""
        with open(file_path, "rb") as handle:
            response = self._client().put_object(
                Bucket=self.config.bucket,
                Key=object_key,
                Body=handle,
            )
        return response.get("ETag", "")

    def _client(self):
        session = boto3.session.Session()
        return session.client(
            "s3",
            endpoint_url=self._endpoint_url(),
            region_name=self.config.region,
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            use_ssl=self.config.use_ssl,
            config=boto3.session.Config(
                s3={"addressing_style": self._addressing_style()}
            ),
        )

    def _endpoint_url(self) -> str | None:
        if not self.config.endpoint:
            return None
        return self.config.endpoint

    def _addressing_style(self) -> str:
        if self.config.force_path_style:
            return "path"
        return "virtual"

