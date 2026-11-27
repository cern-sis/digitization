import pytest

from digitization_workflows.common.storage_connection import S3Provider


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def get_object(self, Bucket, Key):
        self.calls.append((Bucket, Key))
        return {"Body": "stream", "ContentLength": 42}

    def copy(self, CopySource, Bucket, Key):
        self.calls.append(("copy", CopySource, Bucket, Key))

    def delete_object(self, Bucket, Key):
        self.calls.append(("delete", Bucket, Key))


class FailingCopyS3Client(FakeS3Client):
    def copy(self, CopySource, Bucket, Key):
        raise OSError("copy failed")


def provider_with(client):
    provider = S3Provider.__new__(S3Provider)
    provider.bucket = "digitization-dev"
    provider.s3 = client
    return provider


def test_open_stream_returns_the_body_and_its_size():
    provider = S3Provider.__new__(S3Provider)
    provider.bucket = "digitization-dev"
    provider.s3 = FakeS3Client()

    assert provider.open_stream("raw/a.pdf") == ("stream", 42)
    assert provider.s3.calls == [("digitization-dev", "raw/a.pdf")]


def test_move_file_copies_then_deletes_the_original():
    provider = provider_with(FakeS3Client())

    provider.move_file("t/a.pdf", "t/migrated/a.pdf")

    assert provider.s3.calls == [
        (
            "copy",
            {"Bucket": "digitization-dev", "Key": "t/a.pdf"},
            "digitization-dev",
            "t/migrated/a.pdf",
        ),
        ("delete", "digitization-dev", "t/a.pdf"),
    ]


def test_move_file_keeps_the_original_when_the_copy_fails():
    provider = provider_with(FailingCopyS3Client())

    with pytest.raises(OSError):
        provider.move_file("t/a.pdf", "t/migrated/a.pdf")

    assert provider.s3.calls == []
