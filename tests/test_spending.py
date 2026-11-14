import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient, TldwError

JOB = "00000000-0000-4000-8000-000000000101"


class SpendingTests(unittest.TestCase):
    def test_cap_omission_is_decided_by_authenticated_server_without_extra_scope(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((url, json.loads(body)))
            return 202, {}, json.dumps({"job_id": JOB, "job": {"id": JOB}}).encode()

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        client.submit_analysis("source", transcription="if_needed")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0][0].endswith("/v1/analyses"))
        self.assertEqual(calls[0][1], {"media": "source", "transcription": "if_needed"})

    def test_normal_account_policy_rejection_is_not_retried(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append(url)
            return (
                422,
                {},
                json.dumps(
                    {
                        "error": {
                            "code": "TRANSCRIPTION_NOT_ALLOWED",
                            "message": "A credit ceiling is required.",
                        }
                    }
                ).encode(),
            )

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        with self.assertRaises(TldwError) as result:
            client.submit_analysis("source", transcription="if_needed")
        self.assertEqual(result.exception.code, "TRANSCRIPTION_NOT_ALLOWED")
        self.assertEqual(len(calls), 1)

    def test_explicit_spending_contract_and_invalid_caps_never_submit(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append(json.loads(body))
            return 202, {}, json.dumps({"job_id": JOB, "job": {"id": JOB}}).encode()

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        client.submit_analysis(
            "source",
            transcription="if_needed",
            max_credits=Decimal("12.30"),
            force_asr=True,
        )
        self.assertEqual(
            calls[0],
            {
                "media": "source",
                "transcription": "if_needed",
                "max_credits": "12.30",
                "force_asr": True,
            },
        )
        for cap in (0, -1, "NaN", "Infinity", "1.001", "1000000.01", True):
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                client.submit_analysis("source", max_credits=cap)
        with self.assertRaises(ValueError):
            client.submit_analysis("source", force_asr=True)
        self.assertEqual(len(calls), 1)
