"""Download the two public project datasets and upload them to local S3Mock."""

from pathlib import Path
from urllib.request import urlretrieve

import boto3
from botocore.config import Config


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
SOURCE = "https://code.s3.yandex.net/data-scientist/dsplus"
BUCKET = "s3-ds-source"


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9090",
        aws_access_key_id="localminio",
        aws_secret_access_key="localminio123",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    if BUCKET not in [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=BUCKET)

    for name in ("audition.parquet", "content.parquet"):
        path = DATA_DIR / name
        if not path.is_file():
            print(f"Downloading {name}...")
            temporary_path = DATA_DIR / f"{name}.download"
            urlretrieve(f"{SOURCE}/{name}", temporary_path)
            temporary_path.replace(path)
        print(f"Uploading {name} ({path.stat().st_size} bytes)...")
        s3.upload_file(str(path), BUCKET, name)
        print(f"s3://{BUCKET}/{name}")


if __name__ == "__main__":
    main()
