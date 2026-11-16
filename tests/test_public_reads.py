import json
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwCanceledError, TldwClient, TldwError, TldwTimeoutError

API = "https://fixture.invalid"
ID = "00000000-0000-4000-8000-000000000101"
READS = (
    ("get_public_config", "/v1/public/config", {"features": {"x_search": True}}),
    (
        "get_public_security",
        "/v1/public/security",
        {
            "trial_enabled": True,
            "free_monthly_credits": 240,
            "trial_max_video_minutes": 70,
        },
    ),
    (
        "get_billing_catalog",
        "/v1/billing/catalog",
        {
            "billing_enabled": True,
            "plans": [
                {"tier": "hobby", "price_usd": "20.00", "checkout_available": True}
            ],
            "credit_packs": [{"credits": 1000, "price_usd": "20.00"}],
        },
    ),
)


class PublicReadsTests(unittest.TestCase):
    def test_public_mode_is_explicit_and_preserves_default_authentication(self):
        with patch.dict(os.environ, {"TLDW_API_TOKEN": "fixture-must-not-be-read"}):
            with self.assertRaises(ValueError):
                TldwClient(api_url=API)
            TldwClient(api_url=API, public_only=True)
        for value in (None, 1, "true"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                TldwClient(api_url=API, public_only=value)
        with self.assertRaises(ValueError):
            TldwClient(api_url=API, public_only=True, token="fixture")
        for origin in (
            "http://remote.example",
            "https://user:password@api.example",
            "https://api.example/path",
            "https://api.example/?key=value",
        ):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                TldwClient(api_url=origin, public_only=True)

    def test_all_public_reads_preserve_json_and_send_no_auth_without_a_key(self):
        fixtures = {API + path: body for _, path, body in READS}
        for public_only in (True, False):
            with self.subTest(public_only=public_only):
                calls = []

                def transport(method, url, headers, body, timeout, *, _calls=calls):
                    _calls.append((method, url, headers, body))
                    return 200, {}, json.dumps(fixtures[url]).encode()

                options = {"public_only": True} if public_only else {"token": "fixture"}
                client = TldwClient(api_url=API, transport=transport, **options)
                for method, _, value in READS:
                    self.assertEqual(getattr(client, method)(), value)
                self.assertEqual(len(calls), 3)
                for method, _, headers, body in calls:
                    self.assertEqual(method, "GET")
                    self.assertIsNone(body)
                    self.assertEqual(
                        headers.get("Authorization"),
                        None if public_only else "Bearer fixture",
                    )

    def test_private_operations_and_event_streams_never_reach_transport(self):
        calls = []

        def forbidden(*args):
            calls.append(args)
            raise AssertionError("Private operation reached a public-only transport")

        client = TldwClient(
            api_url=API,
            public_only=True,
            transport=forbidden,
            stream_transport=forbidden,
        )
        operations = (
            lambda: client.get_usage(),
            lambda: client.get_job(ID),
            lambda: client.wait_job(ID),
            lambda: client.list_jobs(),
            lambda: client.submit_analysis("source"),
            lambda: client.search_videos("query"),
            lambda: client.cancel_job(ID),
            lambda: client.get_summary(ID),
            lambda: client.get_transcript(ID),
            lambda: client.get_saved_transcript_page(ID),
            lambda: client.mark_transcript_read(ID),
            lambda: client.stream_job_events(ID),
            lambda: client._request("POST", "/v1/public/config"),
            lambda: client._request("GET", "/v1/public/config?private=value"),
            lambda: client._request(
                "GET", "/v1/public/config", body={"private": "value"}
            ),
        )
        for operation in operations:
            with (
                self.subTest(operation=operation),
                self.assertRaises(TldwError) as result,
            ):
                operation()
            self.assertEqual(result.exception.code, "PUBLIC_READ_ONLY")
        self.assertEqual(calls, [])

    def test_public_metadata_refuses_redirects_and_preserves_structured_errors(self):
        for redirect in (True, False):
            with self.subTest(redirect=redirect):
                calls = []

                def transport(
                    method,
                    url,
                    headers,
                    body,
                    timeout,
                    *,
                    _calls=calls,
                    _redirect=redirect,
                ):
                    _calls.append(url)
                    if _redirect:
                        return 302, {"location": "https://other.invalid/data"}, b"{}"
                    return (
                        403,
                        {},
                        json.dumps(
                            {
                                "error": {
                                    "code": "FEATURE_DISABLED",
                                    "message": "Fixture policy",
                                    "details": {"reason": "policy"},
                                }
                            }
                        ).encode(),
                    )

                client = TldwClient(api_url=API, public_only=True, transport=transport)
                with self.assertRaises(TldwError) as result:
                    client.get_public_config()
                self.assertEqual(result.exception.status, 302 if redirect else 403)
                self.assertEqual(
                    result.exception.code,
                    "HTTP_ERROR" if redirect else "FEATURE_DISABLED",
                )
                self.assertEqual(len(calls), 1)

    def test_public_metadata_retains_body_limit_deadline_and_cancellation(self):
        client = TldwClient(
            api_url=API,
            public_only=True,
            transport=lambda *args: (200, {}, b" " * (8 * 1024 * 1024 + 1)),
        )
        with self.assertRaises(TldwError) as result:
            client.get_billing_catalog()
        self.assertEqual(result.exception.code, "INVALID_RESPONSE")
        clock = [0.0]
        calls = []

        def late(method, url, headers, body, timeout):
            calls.append(url)
            clock[0] += 2
            return 200, {}, b"{}"

        client = TldwClient(
            api_url=API,
            public_only=True,
            transport=late,
            request_timeout_seconds=1,
            _clock=lambda: clock[0],
        )
        with self.assertRaises(TldwTimeoutError):
            client.get_public_config()
        self.assertEqual(len(calls), 1)
        canceled = threading.Event()
        canceled.set()
        calls.clear()
        with self.assertRaises(TldwCanceledError):
            client.get_public_security(cancel_event=canceled)
        self.assertEqual(calls, [])
