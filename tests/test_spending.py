import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient

JOB = "00000000-0000-4000-8000-000000000101"


class SpendingTests(unittest.TestCase):
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
