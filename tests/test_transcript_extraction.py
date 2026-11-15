import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient, TldwError, TldwTimeoutError

JOB = "00000000-0000-4000-8000-000000000101"
SUMMARY = "00000000-0000-4000-8000-000000000103"
TRANSCRIPT = "00000000-0000-4000-8000-000000000102"
ACCEPTED = {
    "job_id": JOB,
    "output_mode": "analysis",
    "job": {"id": JOB, "output_mode": "analysis", "status": "queued", "progress": 0},
    "status_url": "https://untrusted.example/never-follow",
}
COMPLETED = {
    "id": JOB,
    "output_mode": "analysis",
    "status": "completed",
    "progress": 100,
    "summary_id": SUMMARY,
    "transcript_id": TRANSCRIPT,
    "result_url": "https://untrusted.example/never-follow",
}
SOURCE = {
    "transcript_id": TRANSCRIPT,
    "source": "platform_manual",
    "language": "en",
    "timed": True,
    "segments": [{"start_ms": 0, "end_ms": 1000, "text": "Original words."}],
    "blocks": [],
    "text": None,
}


def response(value, status=200):
    return status, {}, json.dumps(value).encode()


class TranscriptExtractionTests(unittest.TestCase):
    def client(self, transport):
        return TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )

    def test_analyzes_once_then_reads_original_source_without_untrusted_navigation(
        self,
    ):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((method, url, headers, body))
            if method == "POST":
                return response(ACCEPTED, 202)
            if "/jobs/" in url:
                return response(COMPLETED)
            self.assertEqual(
                url, f"https://api.example.test/v1/summaries/{SUMMARY}/transcript"
            )
            return response(SOURCE)

        result = self.client(transport).extract_transcript(
            "Original source", idempotency_key="extract-once"
        )
        self.assertEqual(result["transcript"], SOURCE)
        self.assertEqual(result["job"]["summary_id"], SUMMARY)
        self.assertEqual(len(calls), 3)
        self.assertEqual(json.loads(calls[0][3]), {"media": "Original source"})
        self.assertEqual(calls[0][2]["Idempotency-Key"], "extract-once")

    def test_extraction_passes_explicit_spending_controls_through_analysis(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            if method == "POST":
                self.assertEqual(url, "https://api.example.test/v1/analyses")
                calls.append(json.loads(body))
                return response(ACCEPTED, 202)
            return response(COMPLETED if "/jobs/" in url else SOURCE)

        client = self.client(transport)
        client.extract_transcript(
            "source",
            transcription="if_needed",
            max_credits="9.00",
            force_asr=True,
            refresh_source=True,
        )
        self.assertEqual(
            calls,
            [
                {
                    "media": "source",
                    "transcription": "if_needed",
                    "max_credits": "9.00",
                    "force_asr": True,
                    "refresh_source": True,
                }
            ],
        )
        self.assertFalse(hasattr(client, "submit_transcript"))

    def test_invalid_inputs_never_submit(self):
        calls = []
        client = self.client(lambda *args: calls.append(args))
        for media, options in [
            (None, {}),
            ("source", {"preferred_caption_languages": ["invalid tag"]}),
            ("source", {"force_asr": True}),
        ]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                client.extract_transcript(media, **options)
        self.assertEqual(calls, [])
        with self.assertRaises(ValueError):
            client.extract_transcript("source", timeout_seconds=-1)
        self.assertEqual(calls, [])

    def test_output_mode_selects_required_result_id(self):
        for value in (None, "https://bad.example/source", 42):
            client = self.client(
                lambda *args, value=value: response(
                    {
                        **COMPLETED,
                        "output_mode": "transcript",
                        "summary_id": None,
                        "transcript_id": value,
                    }
                )
            )
            with self.subTest(value=value), self.assertRaises(TldwError) as caught:
                client.wait_for_job(JOB)
            self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
        client = self.client(lambda *args: response({**COMPLETED, "summary_id": None}))
        with self.assertRaises(TldwError):
            client.wait_for_job(JOB)

    def test_source_scope_failure_keeps_receipt_and_durable_job(self):
        def transport(method, url, *args):
            if method == "POST":
                return response(ACCEPTED, 202)
            if "/jobs/" in url:
                return response(COMPLETED)
            return response({"error": {"code": "TOKEN_SCOPE_REQUIRED"}}, 403)

        with self.assertRaises(TldwError) as caught:
            self.client(transport).extract_transcript("source")
        self.assertEqual(caught.exception.code, "TOKEN_SCOPE_REQUIRED")
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(caught.exception.receipt["job_id"], JOB)

    def test_wait_timeout_does_not_cancel_or_resubmit_accepted_extraction(self):
        calls = []

        def transport(*args):
            calls.append(args)
            return response({**ACCEPTED, "next_poll_after_ms": 3000}, 202)

        with self.assertRaises(TldwTimeoutError) as caught:
            self.client(transport).extract_transcript("source", timeout_seconds=0.001)
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(caught.exception.receipt["job_id"], JOB)
        self.assertEqual(len(calls), 1)

    def test_invalid_receipt_retains_stable_submission_key(self):
        client = self.client(
            lambda *args: response({**ACCEPTED, "job": {"id": TRANSCRIPT}}, 202)
        )
        with self.assertRaises(TldwError) as caught:
            client.submit_analysis("source", idempotency_key="stable-key")
        self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
        self.assertEqual(caught.exception.idempotency_key, "stable-key")
