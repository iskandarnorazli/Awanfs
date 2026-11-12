#!/usr/bin/env python3
"""
awan-storage/server/tests/test_boto3_compat.py
Boto3 S3 Integration and Negative Control Verification Suite (#464)
"""

import hashlib
import os
import socket
import subprocess
import sys
import time
import unittest
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

class TestBoto3Compat(unittest.TestCase):
    server_process = None
    port = None
    s3 = None

    @classmethod
    def setUpClass(cls):
        cls.port = get_free_port()
        # Compile server binary if needed
        server_bin = os.path.join(
            os.path.dirname(__file__), "..", "target", "debug",
            "awan-storage-server.exe" if sys.platform == "win32" else "awan-storage-server"
        )
        if not os.path.exists(server_bin):
            build_cmd = ["cargo", "build", "--bin", "awan-storage-server"]
            manifest = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Cargo.toml"))
            subprocess.run(build_cmd + ["--manifest-path", manifest], check=True)

        cls.server_process = subprocess.Popen(
            [server_bin, "--port", str(cls.port), "--allow-anonymous"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Wait for server to listen
        time.sleep(1.0)

        cls.endpoint = f"http://127.0.0.1:{cls.port}"
        cls.s3 = boto3.client(
            "s3",
            endpoint_url=cls.endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(
                s3={"addressing_style": "path"},
                signature_version="s3v4",
                retries={"max_attempts": 1},
            ),
        )

    @classmethod
    def tearDownClass(cls):
        if cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait()

    def test_01_bucket_crud(self):
        bucket = "boto3-test-bucket-01"
        self.s3.create_bucket(Bucket=bucket)
        self.s3.head_bucket(Bucket=bucket)

    def test_02_put_and_get_odd_sizes(self):
        bucket = "boto3-test-bucket-01"
        sizes = [0, 1, 64 * 1024, 1024 * 1024]
        for sz in sizes:
            key = f"object_size_{sz}.bin"
            payload = os.urandom(sz)
            md5_expected = hashlib.md5(payload).hexdigest()

            put_res = self.s3.put_object(Bucket=bucket, Key=key, Body=payload)
            self.assertIn("ETag", put_res)

            get_res = self.s3.get_object(Bucket=bucket, Key=key)
            body = get_res["Body"].read()
            self.assertEqual(len(body), sz)
            self.assertEqual(body, payload)
            self.assertEqual(hashlib.md5(body).hexdigest(), md5_expected)

    def test_03_range_requests(self):
        bucket = "boto3-test-bucket-01"
        key = "range_test.txt"
        content = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        self.s3.put_object(Bucket=bucket, Key=key, Body=content)

        # Byte range 10-19 -> "ABCDEFGHIJ"
        res = self.s3.get_object(Bucket=bucket, Key=key, Range="bytes=10-19")
        slice_data = res["Body"].read()
        self.assertEqual(slice_data, b"ABCDEFGHIJ")

    def test_04_copy_object(self):
        bucket = "boto3-test-bucket-01"
        src_key = "source.dat"
        dst_key = "destination.dat"
        data = b"Atomic CopyObject Payload"
        self.s3.put_object(Bucket=bucket, Key=src_key, Body=data)

        self.s3.copy_object(
            Bucket=bucket,
            Key=dst_key,
            CopySource={"Bucket": bucket, "Key": src_key},
        )

        res = self.s3.get_object(Bucket=bucket, Key=dst_key)
        self.assertEqual(res["Body"].read(), data)

    def test_05_list_objects_v2(self):
        bucket = "boto3-test-bucket-01"
        self.s3.put_object(Bucket=bucket, Key="logs/2026/01.log", Body=b"log1")
        self.s3.put_object(Bucket=bucket, Key="logs/2026/02.log", Body=b"log2")
        self.s3.put_object(Bucket=bucket, Key="photos/cat.jpg", Body=b"cat")

        res = self.s3.list_objects_v2(Bucket=bucket, Prefix="logs/")
        keys = [item["Key"] for item in res.get("Contents", [])]
        self.assertIn("logs/2026/01.log", keys)
        self.assertIn("logs/2026/02.log", keys)
        self.assertNotIn("photos/cat.jpg", keys)

    def test_06_multipart_upload_workflow(self):
        bucket = "boto3-test-bucket-01"
        key = "large_multipart.dat"
        part1 = b"A" * (5 * 1024 * 1024)
        part2 = b"B" * (5 * 1024 * 1024)

        mp = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]

        res1 = self.s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1
        )
        res2 = self.s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2
        )

        parts = [
            {"PartNumber": 1, "ETag": res1["ETag"]},
            {"PartNumber": 2, "ETag": res2["ETag"]},
        ]

        self.s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )

        read_res = self.s3.get_object(Bucket=bucket, Key=key)
        full_body = read_res["Body"].read()
        self.assertEqual(len(full_body), 10 * 1024 * 1024)
        self.assertEqual(full_body, part1 + part2)

    def test_07_corrupted_etag_negative_control(self):
        """Negative control: tampering with an ETag during CompleteMultipartUpload must be rejected"""
        bucket = "boto3-test-bucket-01"
        try:
            self.s3.create_bucket(Bucket=bucket)
        except Exception:
            pass
        key = "corrupted_etag_target.dat"
        part = b"C" * (5 * 1024 * 1024)

        mp = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]
        self.s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part
        )

        # Pass a bogus corrupted ETag
        corrupted_parts = [{"PartNumber": 1, "ETag": '"00000000000000000000000000000000"'}]

        with self.assertRaises(ClientError) as ctx:
            self.s3.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": corrupted_parts}
            )
        err_code = ctx.exception.response["Error"]["Code"]
        self.assertIn(err_code, ["InvalidPart", "400"])

        # Abort upload cleanup
        self.s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

    def test_08_batch_delete_and_bucket_cleanup(self):
        bucket = "boto3-test-bucket-01"
        # List all remaining objects and delete them
        res = self.s3.list_objects_v2(Bucket=bucket)
        objects = [{"Key": item["Key"]} for item in res.get("Contents", [])]
        if objects:
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

        self.s3.delete_bucket(Bucket=bucket)

if __name__ == "__main__":
    unittest.main()
