import json
import sys
import unittest
from http.client import BadStatusLine
from pathlib import Path
from threading import Event
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import (
    TldwCanceledError,
    TldwClient,
    TldwError,
    TldwJobError,
    TldwTimeoutError,
)

JOB = "00000000-0000-4000-8000-000000000101"
SUMMARY = "00000000-0000-4000-8000-000000000102"
TOKEN = "tldw_fixture_not_a_real_secret"


def response(value, status=200, **headers):
    return status, headers, json.dumps(value).encode()


def job(status="queued", **extra):
    return {
        "id": JOB,
        "status": status,
        "progress": 100 if status == "completed" else 20,
        "summary_id": SUMMARY if status == "completed" else None,
        **extra,
    }


class ClientTests(unittest.TestCase):
    def test_transcript_is_a_separate_owned_read_with_both_source_representations(self):
        for timed in (True, False):
            source = {
                "transcript_id": JOB,
                "language": "en",
                "source": "platform_auto",
                "timed": timed,
                "segments": [{"start_ms": 0, "end_ms": 1000, "text": "Hello."}]
                if timed
                else [],
                "blocks": []
                if timed
                else [
                    {
                        "id": "caption-1",
                        "label": "Video captions",
                        "text": "Hello.",
                        "start_s": 0,
                        "end_s": 1,
                    }
                ],
                "text": None if timed else "Hello.",
            }

            def transport(method, url, headers, body, timeout, source=source):
                self.assertEqual(method, "GET")
                self.assertEqual(
                    url, f"https://api.example.test/v1/summaries/{SUMMARY}/transcript"
                )
                self.assertEqual(headers["Authorization"], "Bearer " + TOKEN)
                return response(source)

            self.assertEqual(self.client(transport).get_transcript(SUMMARY), source)

    def test_transcript_rejects_url_ids_and_invalid_success_payloads(self):
        calls = []

        def transport(*args):
            calls.append(args)
            return response({"transcript_id": JOB})

        client = self.client(transport)
        with self.assertRaises(ValueError):
            client.get_transcript("https://evil.invalid/steal")
        self.assertEqual(calls, [])
        with self.assertRaises(TldwError) as caught:
            client.get_transcript(SUMMARY)
        self.assertEqual(caught.exception.code, "INVALID_RESPONSE")

    def test_transcript_forbidden_response_is_not_returned_as_source(self):
        client = self.client(
            lambda *args: response({"error": {"code": "TOKEN_SCOPE_REQUIRED"}}, 403)
        )
        with self.assertRaises(TldwError) as caught:
            client.get_transcript(SUMMARY)
        self.assertEqual(caught.exception.code, "TOKEN_SCOPE_REQUIRED")

    def client(self, transport, **options):
        self.now = 0

        def advance(seconds):
            self.now += seconds

        return TldwClient(
            api_url="https://api.example.test",
            token=TOKEN,
            transport=transport,
            _clock=lambda: self.now,
            _sleep=advance,
            **options,
        )

    def test_requires_strict_origin_and_explicit_local_anonymous_mode(self):
        for url in (
            "http://example.test",
            "https://u:p@example.test",
            "https://example.test/path",
            "https://example.test?key=x",
            "https://example.test#x",
            "file:///tmp",
            "http://localhost.evil.test",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                TldwClient(api_url=url, token=TOKEN)
        with self.assertRaises(ValueError):
            TldwClient(api_url="https://api.example.test", allow_anonymous=True)
        with self.assertRaises(ValueError):
            TldwClient(api_url="http://localhost:8000")
        TldwClient(api_url="http://localhost:8000", allow_anonymous=True)

    def test_successful_analysis_uses_fixed_origin_and_real_contract(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((method, url, headers, body))
            if method == "POST":
                return response(
                    {
                        "job_id": JOB,
                        "job": job(),
                        "status_url": "https://evil.test/capture",
                    },
                    202,
                )
            if "/jobs/" in url:
                return response(job("completed"))
            return response({"id": SUMMARY, "content": {"answer": "An answer"}})

        result = self.client(transport).analyze(
            "source", instructions="Extract claims", idempotency_key="stable-1"
        )
        self.assertEqual(result["summary"]["content"]["answer"], "An answer")
        self.assertEqual(
            json.loads(calls[0][3]),
            {"media": "source", "instructions": "Extract claims"},
        )
        self.assertEqual(calls[0][2]["Idempotency-Key"], "stable-1")
        self.assertEqual(calls[0][2]["Authorization"], "Bearer " + TOKEN)
        self.assertTrue(
            all(call[1].startswith("https://api.example.test/v1/") for call in calls)
        )

    def test_transient_post_retries_are_idempotent_and_body_stable(self):
        source = {"type": "text", "text": "original"}
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((headers, body))
            source["text"] = "changed externally"
            if len(calls) == 1:
                return response(
                    {"error": {"code": "RATE_LIMITED"}}, 429, **{"retry-after": "2"}
                )
            return response({"job_id": JOB, "job": job()}, 202)

        self.client(transport).submit_analysis(source)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(self.now, 2)

    def test_unknown_submission_failure_keeps_stable_key(self):
        def transport(*args):
            raise URLError("private transport debug text")

        client = self.client(transport)
        with self.assertRaises(TldwError) as caught:
            client.submit_analysis("source", idempotency_key="resume-me")
        self.assertEqual(caught.exception.idempotency_key, "resume-me")
        self.assertNotIn("private transport", str(caught.exception))

    def test_custom_transport_cannot_succeed_after_deadline(self):
        def transport(*args):
            self.now += 2
            return response({"job_id": JOB, "job": job()}, 202)

        client = self.client(transport, request_timeout_seconds=1)
        with self.assertRaises(TldwTimeoutError) as caught:
            client.submit_analysis("source", idempotency_key="late-reply")
        self.assertEqual(caught.exception.idempotency_key, "late-reply")

    def test_custom_transport_protocol_failure_keeps_submission_key(self):
        def transport(*args):
            raise BadStatusLine("private protocol debug text")

        client = self.client(transport)
        with self.assertRaises(TldwError) as caught:
            client.submit_analysis("A complete source")
        self.assertEqual(caught.exception.code, "NETWORK_ERROR")
        self.assertTrue(caught.exception.idempotency_key)
        self.assertNotIn("private protocol", str(caught.exception))

    def test_rate_limit_honors_deadline_and_retains_key(self):
        client = self.client(
            lambda *args: response(
                {"error": {"code": "RATE_LIMITED"}}, 429, **{"retry-after": "60"}
            ),
            request_timeout_seconds=1,
        )
        with self.assertRaises(TldwError) as caught:
            client.submit_analysis("source", idempotency_key="same-body")
        self.assertEqual(caught.exception.idempotency_key, "same-body")
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.retry_after_seconds, 60)
        self.assertEqual(self.now, 0)

    def test_waiter_remembers_submission_poll_hint(self):
        calls = []

        def transport(method, *args):
            calls.append(method)
            return response(
                {"job_id": JOB, "job": job(), "next_poll_after_ms": 3000}, 202
            )

        client = self.client(transport)
        receipt = client.submit_analysis("source")
        with self.assertRaises(TldwTimeoutError) as caught:
            client.wait_for_job(receipt["job_id"], timeout_seconds=0.01)
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(calls, ["POST"])

    def test_invalid_wait_options_do_not_submit_paid_work(self):
        calls = []

        def transport(*args):
            calls.append(args)
            return response({"job_id": JOB, "job": job()}, 202)

        client = self.client(transport)
        with self.assertRaises(ValueError):
            client.analyze("source", timeout_seconds=-1)
        with self.assertRaises(ValueError):
            client.analyze("source", on_progress="invalid")
        self.assertEqual(calls, [])

    def test_wait_uses_progress_and_server_poll_hint(self):
        progress = []
        polls = []

        def transport(*args):
            polls.append(self.now)
            return response(
                job("queued", next_poll_after_ms=3000)
                if len(polls) == 1
                else job("completed")
            )

        result = self.client(transport).wait_for_job(
            JOB, on_progress=lambda value: progress.append(value["status"])
        )
        self.assertEqual(result["summary_id"], SUMMARY)
        self.assertEqual(polls, [0, 3])
        self.assertEqual(progress, ["queued", "completed"])

    def test_wait_treats_queued_for_slot_as_not_terminal(self):
        progress = []

        def transport(*args):
            return response(
                job("queued_for_slot", queue_position=1, max_concurrent_jobs=1)
                if not progress
                else job("completed")
            )

        result = self.client(transport).wait_for_job(
            JOB, on_progress=lambda value: progress.append(value["status"])
        )
        self.assertEqual(result["summary_id"], SUMMARY)
        self.assertEqual(progress, ["queued_for_slot", "completed"])

    def test_wait_failure_carries_durable_id(self):
        for status in ("failed", "canceled"):
            with self.subTest(status=status):
                client = self.client(
                    lambda *args, status=status: response(
                        job(
                            status,
                            error={
                                "code": "SOURCE_UNAVAILABLE",
                                "message": "Source removed",
                            },
                        )
                    )
                )
                with self.assertRaises(TldwJobError) as caught:
                    client.wait_for_job(JOB)
                self.assertEqual(caught.exception.job_id, JOB)
                self.assertEqual(caught.exception.code, "SOURCE_UNAVAILABLE")

    def test_canceled_wait_never_sends_server_cancellation(self):
        event = Event()
        calls = []

        def transport(method, *args):
            calls.append(method)
            return response(job())

        client = self.client(transport)
        with self.assertRaises(TldwCanceledError) as caught:
            client.wait_for_job(
                JOB, cancel_event=event, on_progress=lambda value: event.set()
            )
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(calls, ["GET"])

    def test_terminal_error_redacts_reflected_key(self):
        client = self.client(
            lambda *args: response(
                job(
                    "failed",
                    error={"code": "SOURCE_UNAVAILABLE", "message": "Debug " + TOKEN},
                )
            )
        )
        with self.assertRaises(TldwJobError) as caught:
            client.wait_for_job(JOB)
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertNotIn(TOKEN, json.dumps(caught.exception.resource))

    def test_convenience_timeout_retains_receipt(self):
        client = self.client(
            lambda method, *args: response(
                {"job_id": JOB, "job": job()} if method == "POST" else job()
            )
        )
        with self.assertRaises(TldwTimeoutError) as caught:
            client.analyze("source", timeout_seconds=0.001)
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(caught.exception.receipt["job_id"], JOB)

    def test_invalid_completed_resource_cannot_inject_request_path(self):
        calls = []

        def transport(*args):
            calls.append(args)
            return response(job("completed", summary_id="https://evil.test/capture"))

        client = self.client(transport)
        with self.assertRaises(TldwError):
            client.wait_for_job(JOB)
        with self.assertRaises(ValueError):
            client.get_job("../admin")
        self.assertEqual(len(calls), 1)

    def test_http_metadata_and_token_redaction(self):
        client = self.client(
            lambda *args: response(
                {
                    "error": {
                        "code": "SCOPE_REQUIRED",
                        "message": TOKEN + " missing scope",
                        "request_id": "request-123",
                    }
                },
                403,
            )
        )
        with self.assertRaises(TldwError) as caught:
            client.get_usage()
        error = caught.exception
        self.assertEqual(
            (error.code, error.status, error.request_id),
            ("SCOPE_REQUIRED", 403, "request-123"),
        )
        self.assertNotIn(TOKEN, str(error))
        self.assertNotIn(TOKEN, repr(client))

    def test_client_exposes_no_comparison_methods(self):
        client = self.client(lambda *args: response({}))
        for name in ("compare_tiktoks", "get_comparison", "wait_for_comparison"):
            self.assertFalse(hasattr(client, name), name)

    def test_invalid_json_and_oversized_responses_fail_safely(self):
        for body in (b"not-json", b" " * (8 * 1024 * 1024 + 1)):
            client = self.client(lambda *args, body=body: (200, {}, body))
            with self.assertRaises(TldwError) as caught:
                client.get_usage()
            self.assertEqual(caught.exception.code, "INVALID_RESPONSE")

    def test_api_error_details_are_redacted_and_give_the_active_job(self):
        active = "00000000-0000-4000-8000-000000000199"

        def transport(*_):
            return response(
                {
                    "error": {
                        "code": "DUPLICATE_ACTIVE_JOB",
                        "message": "That video is already processing.",
                        "details": {"job_id": active, "note": f"seen {TOKEN}"},
                    }
                },
                409,
            )

        with self.assertRaises(TldwError) as caught:
            self.client(transport).submit_analysis("A complete source")
        self.assertEqual(caught.exception.code, "DUPLICATE_ACTIVE_JOB")
        self.assertEqual(caught.exception.job_id, active)
        self.assertEqual(
            caught.exception.details, {"job_id": active, "note": "seen [REDACTED]"}
        )

    def test_one_slow_poll_does_not_end_a_longer_wait(self):
        calls = []

        def transport(method, url, headers, payload, timeout):
            calls.append(method)
            if len(calls) == 1:
                self.now += timeout
                raise TimeoutError()
            return response(job("completed"))

        client = self.client(transport, request_timeout_seconds=1)
        self.assertEqual(
            client.wait_for_job(JOB, timeout_seconds=600)["status"], "completed"
        )
        self.assertGreaterEqual(len(calls), 2)

    def test_a_slow_submission_is_not_retried_and_says_how_to_retry(self):
        def transport(*_):
            raise TimeoutError()

        with self.assertRaises(TldwTimeoutError) as caught:
            self.client(transport).submit_analysis("A complete source")
        self.assertIn("same idempotency key", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
