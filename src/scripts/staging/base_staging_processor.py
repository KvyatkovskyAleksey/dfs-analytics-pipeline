import os

from scripts.exceptions import ImproperlyConfigured


class BaseStagingProcessor:
    def __init__(self) -> None:
        self.s3_endpoint = os.getenv("WASABI_ENDPOINT", "s3.us-east-2.wasabisys.com")
        self.s3_access_key_id = os.getenv("WASABI_ACCESS_KEY")
        self.s3_secret_access_key = os.getenv("WASABI_SECRET_KEY")
        self.bucket_name = os.getenv("WASABI_BUCKET_NAME")
        if (
            not self.s3_access_key_id
            or not self.s3_secret_access_key
            or not self.bucket_name
        ):
            raise ImproperlyConfigured(
                "WASABI_ACCESS_KEY, WASABI_SECRET_KEY and WASABI_BUCKET_NAME environment variables must be set"
            )
