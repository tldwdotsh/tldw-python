import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient, TldwError, TldwJobError

JOB = "00000000-0000-4000-8000-000000000101"


class SpendingTests(unittest.TestCase):
    def test_omission_auto_and_credit_cap_do_not_opt_in_to_fresh_asr(self):
        bodies = []

        def transport(method, url, headers, body, timeout):
            bodies.append(json.loads(body))
            return (
                202,
                {},
                json.dumps(
                    {
                        "job_id": JOB,
                        "job": {"id": JOB, "status": "queued", "progress": 0},
                    }
                ).encode(),
            )

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        client.submit_analysis("source")
        client.submit_analysis("source", transcription="auto")
        client.submit_analysis("source", max_credits="30.00")
        client.submit_analysis("source", transcription="never")
        self.assertEqual(
            bodies,
            [
                {"media": "source"},
                {"media": "source", "transcription": "auto"},
                {"media": "source", "max_credits": "30.00"},
                {"media": "source", "transcription": "never"},
            ],
        )
        with self.assertRaises(ValueError):
            client.submit_analysis("source", transcription="sometimes")
        self.assertEqual(len(bodies), 4)

    def test_legacy_if_needed_has_no_artificial_cap_requirement_or_extra_scope(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((url, json.loads(body)))
            return (
                202,
                {},
                json.dumps(
                    {
                        "job_id": JOB,
                        "job": {"id": JOB, "status": "queued", "progress": 0},
                    }
                ).encode(),
            )

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        client.submit_analysis("source", transcription="if_needed")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0][0].endswith("/v1/analyses"))
        self.assertEqual(calls[0][1], {"media": "source", "transcription": "if_needed"})

    def test_normal_credit_ceiling_rejection_is_not_retried(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append(url)
            return (
                422,
                {},
                json.dumps(
                    {
                        "error": {
                            "code": "MAX_CREDITS_EXCEEDED",
                            "message": "The source exceeds the credit ceiling.",
                        }
                    }
                ).encode(),
            )

        client = TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )
        with self.assertRaises(TldwError) as result:
            client.submit_analysis("source", max_credits="1.00")
        self.assertEqual(result.exception.code, "MAX_CREDITS_EXCEEDED")
        self.assertEqual(len(calls), 1)

    def test_missing_captions_are_structured_and_nonretryable(self):
        details = {"platform": "youtube", "reason": "captions_unavailable"}
        for status in (422, 503):
            with self.subTest(status=status):
                bodies = []

                def transport(
                    method,
                    url,
                    headers,
                    body,
                    timeout,
                    *,
                    _bodies=bodies,
                    _status=status,
                ):
                    _bodies.append(json.loads(body))
                    return (
                        _status,
                        {},
                        json.dumps(
                            {
                                "error": {
                                    "code": "CAPTIONS_UNAVAILABLE",
                                    "message": "No accessible captions were found.",
                                    "details": details,
                                }
                            }
                        ).encode(),
                    )

                client = TldwClient(
                    api_url="https://api.example.test",
                    token="fixture",
                    transport=transport,
                )
                with self.assertRaises(TldwError) as result:
                    client.submit_analysis("source", max_credits="30.00")
                self.assertEqual(result.exception.code, "CAPTIONS_UNAVAILABLE")
                self.assertEqual(result.exception.status, status)
                self.assertEqual(result.exception.details, details)
                self.assertEqual(bodies, [{"media": "source", "max_credits": "30.00"}])

    def test_accepted_caption_only_failure_never_creates_an_asr_replacement(self):
        for operation in ("analyze", "extract_transcript"):
            with self.subTest(operation=operation):
                calls = []

                def transport(method, url, headers, body, timeout, *, _calls=calls):
                    _calls.append((method, url, json.loads(body) if body else None))
                    if method == "POST":
                        value = {
                            "job_id": JOB,
                            "job": {"id": JOB, "status": "queued", "progress": 0},
                        }
                        return 202, {}, json.dumps(value).encode()
                    value = {
                        "id": JOB,
                        "status": "failed",
                        "progress": 10,
                        "summary_id": None,
                        "error": {
                            "code": "CAPTIONS_UNAVAILABLE",
                            "message": "No accessible captions were found.",
                            "details": {
                                "platform": "youtube",
                                "reason": "captions_unavailable",
                            },
                        },
                    }
                    return 200, {}, json.dumps(value).encode()

                client = TldwClient(
                    api_url="https://api.example.test",
                    token="fixture",
                    transport=transport,
                )
                with self.assertRaises(TldwJobError) as result:
                    getattr(client, operation)("source")
                self.assertEqual(result.exception.code, "CAPTIONS_UNAVAILABLE")
                self.assertEqual(result.exception.job_id, JOB)
                self.assertEqual(result.exception.receipt["job_id"], JOB)
                self.assertEqual(
                    result.exception.resource["error"]["details"]["reason"],
                    "captions_unavailable",
                )
                self.assertEqual(
                    calls,
                    [
                        (
                            "POST",
                            "https://api.example.test/v1/analyses",
                            {"media": "source"},
                        ),
                        ("GET", f"https://api.example.test/v1/jobs/{JOB}", None),
                    ],
                )

    def test_legacy_audio_fields_stay_compatible_and_invalid_caps_never_submit(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append(json.loads(body))
            return (
                202,
                {},
                json.dumps(
                    {
                        "job_id": JOB,
                        "job": {"id": JOB, "status": "queued", "progress": 0},
                    }
                ).encode(),
            )

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
        client.submit_analysis("source", force_asr=True)
        self.assertEqual(calls[1], {"media": "source", "force_asr": True})
        client.submit_analysis("source", transcription="never", force_asr=True)
        self.assertEqual(
            calls[2], {"media": "source", "transcription": "never", "force_asr": True}
        )
        self.assertEqual(len(calls), 3)
