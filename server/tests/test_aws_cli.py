#!/usr/bin/env python3
"""
awan-storage/server/tests/test_aws_cli.py
AWS CLI Integration Suite (#464)
"""

import os
import shutil
import socket
import subprocess
import sys
import time
import unittest

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

class TestAwsCliCompat(unittest.TestCase):
    server_process = None
    port = None
    aws_cmd = None

    @classmethod
    def setUpClass(cls):
        cls.aws_cmd = shutil.which("aws")
        cls.port = get_free_port()

        server_bin = os.path.join(
            os.path.dirname(__file__), "..", "target", "debug",
            "awan-storage-server.exe" if sys.platform == "win32" else "awan-storage-server"
        )
        if not os.path.exists(server_bin):
            build_cmd = ["cargo", "build", "--bin", "awan-storage-server"]
            manifest = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Cargo.toml"))
            subprocess.run(build_cmd + ["--manifest-path", manifest], check=True)

        cls.server_process = subprocess.Popen(
            [server_bin, "--port", str(cls.port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(1.0)
        cls.endpoint = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        if cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait()

    def run_aws(self, args):
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = "test"
        env["AWS_SECRET_ACCESS_KEY"] = "test"
        env["AWS_DEFAULT_REGION"] = "us-east-1"
        cmd = [self.aws_cmd, "--endpoint-url", self.endpoint] + args
        res = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return res

    def test_aws_cli_mb_cp_ls_rb(self):
        if not self.aws_cmd:
            self.skipTest("aws-cli executable not found in PATH; skipping live aws-cli test")

        bucket = "s3://cli-test-bucket-01"
        # 1. mb
        res_mb = self.run_aws(["s3", "mb", bucket])
        self.assertEqual(res_mb.returncode, 0, f"mb failed: {res_mb.stderr}")

        # 2. cp
        tmp_file = os.path.join(os.path.dirname(__file__), "test_file.txt")
        with open(tmp_file, "w") as f:
            f.write("AWS CLI Transfer Verification Payload")

        try:
            res_cp = self.run_aws(["s3", "cp", tmp_file, f"{bucket}/test_file.txt"])
            self.assertEqual(res_cp.returncode, 0, f"cp failed: {res_cp.stderr}")

            # 3. ls
            res_ls = self.run_aws(["s3", "ls", bucket])
            self.assertEqual(res_ls.returncode, 0, f"ls failed: {res_ls.stderr}")
            self.assertIn("test_file.txt", res_ls.stdout)

            # 4. rb --force
            res_rb = self.run_aws(["s3", "rb", bucket, "--force"])
            self.assertEqual(res_rb.returncode, 0, f"rb failed: {res_rb.stderr}")
        finally:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

if __name__ == "__main__":
    unittest.main()
