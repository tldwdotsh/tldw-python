import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient

JOB = "00000000-0000-4000-8000-000000000101"
SUMMARY = "00000000-0000-4000-8000-000000000102"


class SourceRefreshTests(unittest.TestCase):
    def client(self, calls):
        def transport(method, url, headers, body, timeout):
            if method == "POST":
                calls.append(json.loads(body))
                value = {
                    "job_id": JOB,
                    "job": {"id": JOB, "status": "queued", "progress": 0},
                }
                return 202, {}, json.dumps(value).encode()
            if "/v1/jobs/" in url:
                value = {
                    "id": JOB,
                    "status": "completed",
                    "progress": 100,
                    "summary_id": SUMMARY,
                }
            else:
                value = {
                    "id": SUMMARY,
                    "content": {"answer": "Stored source analysis."},
                }
            return 200, {}, json.dumps(value).encode()

        return TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )

    def test_source_refresh_is_explicit_and_independent_of_asr_permission(self):
        calls = []
        client = self.client(calls)
        client.submit_analysis("https://x.com/user/status/1234567890")
        client.submit_analysis(
            "https://x.com/user/status/1234567890", refresh_source=False
        )
        client.analyze("https://x.com/user/status/1234567890", refresh_source=True)
        self.assertNotIn("refresh_source", calls[0])
        self.assertIs(calls[1]["refresh_source"], False)
        self.assertIs(calls[2]["refresh_source"], True)
        self.assertTrue(
            all(
                "transcription" not in body and "force_asr" not in body
                for body in calls
            )
        )

    def test_invalid_source_refresh_never_submits(self):
        calls = []
        client = self.client(calls)
        for value in ("true", "false", 1, 0, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                client.submit_analysis("source", refresh_source=value)
        self.assertEqual(calls, [])
