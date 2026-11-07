import json
import sys
import unittest
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
COMPARISON = "00000000-0000-4000-8000-000000000103"
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

    def test_comparison_unwraps_and_checks_result_access(self):
        value = {
            "id": COMPARISON,
            "status": "completed",
            "result": {"summary": "Compared speech", "findings": [], "limitations": []},
        }
        client = self.client(lambda *args: response({"comparison": value}))
        self.assertEqual(client.compare_tiktoks(["url1", "url2"])["id"], COMPARISON)
        self.assertEqual(
            client.wait_for_comparison(COMPARISON)["result"]["summary"],
            "Compared speech",
        )
        value["result"] = None
        with self.assertRaises(TldwError) as caught:
            client.wait_for_comparison(COMPARISON)
        self.assertEqual(caught.exception.code, "RESULT_SCOPE_REQUIRED")

    def test_invalid_json_and_oversized_responses_fail_safely(self):
        for body in (b"not-json", b" " * (8 * 1024 * 1024 + 1)):
            client = self.client(lambda *args, body=body: (200, {}, body))
            with self.assertRaises(TldwError) as caught:
                client.get_usage()
            self.assertEqual(caught.exception.code, "INVALID_RESPONSE")


if __name__ == "__main__":
    unittest.main()
