import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import DEFAULT_API_URL, TldwClient, TldwError

JOB_ID = "00000000-0000-4000-8000-000000000101"
JOB = {"id": JOB_ID, "status": "queued", "progress": 0, "summary_id": None}
API = "https://api.example.test"


def response(value, status=200):
    return status, {}, json.dumps(value).encode()


class JobTests(unittest.TestCase):
    def test_default_origin_and_overrides_never_read_a_token_implicitly(self):
        with patch.dict(
            os.environ, {"TLDW_API_TOKEN": "private-env-sentinel"}, clear=True
        ):
            self.assertEqual(TldwClient(token="explicit").api_url, DEFAULT_API_URL)
            with self.assertRaises(ValueError):
                TldwClient()
            os.environ["TLDW_API_URL"] = API
            self.assertEqual(TldwClient(token="explicit").api_url, API)
            self.assertEqual(
                TldwClient(
                    token="explicit", api_url="https://other.example.test"
                ).api_url,
                "https://other.example.test",
            )

    def test_owned_job_page_uses_bounded_and_encoded_query_values(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            calls.append((method, url, headers, body))
            return response({"items": [JOB], "next_cursor": "next+/="})

        client = TldwClient(api_url=API, token="explicit", transport=transport)
        self.assertEqual(
            client.list_jobs(limit=7, cursor="opaque+/=?&cursor=other")["items"], [JOB]
        )
        method, url, headers, body = calls[0]
        self.assertIsNone(body)
        self.assertEqual(method, "GET")
        self.assertEqual(urlsplit(url).path, "/v1/jobs")
        self.assertEqual(
            parse_qs(urlsplit(url).query),
            {"limit": ["7"], "cursor": ["opaque+/=?&cursor=other"]},
        )
        self.assertEqual(headers["Authorization"], "Bearer explicit")
        for options in (
            {"limit": 0},
            {"limit": 101},
            {"limit": True},
            {"cursor": ""},
            {"cursor": "x" * 1025},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                client.list_jobs(**options)
        self.assertEqual(len(calls), 1)

    def test_malformed_job_pages_fail_validation(self):
        for page in (
            None,
            [],
            {"items": [], "next_cursor": []},
            {"items": [], "next_cursor": ""},
            {"items": [{}], "next_cursor": None},
            {"items": [dict(JOB, status="unknown")], "next_cursor": None},
            {"items": [JOB]},
        ):
            client = TldwClient(
                api_url=API,
                token="explicit",
                transport=lambda *args, page=page: response(page),
            )
            with self.subTest(page=page), self.assertRaises(TldwError) as caught:
                client.list_jobs()
            self.assertEqual(caught.exception.code, "INVALID_RESPONSE")

    def test_explicit_cancel_accepts_terminal_races(self):
        calls = []
        state = {"status": "canceled"}

        def transport(method, url, headers, body, timeout):
            calls.append((method, url, body))
            return response({"id": JOB_ID, "status": state["status"]})

        client = TldwClient(api_url=API, token="explicit", transport=transport)
        for status in ("canceled", "completed", "failed"):
            state["status"] = status
            self.assertEqual(client.cancel_job(JOB_ID)["status"], status)
            self.assertEqual(
                calls[-1], ("POST", f"{API}/v1/jobs/{JOB_ID}/cancel", None)
            )
        with self.assertRaises(ValueError):
            client.cancel_job("https://untrusted.invalid/job")
        self.assertEqual(len(calls), 3)
        state["status"] = "queued"
        with self.assertRaises(TldwError) as caught:
            client.cancel_job(JOB_ID)
        self.assertEqual(caught.exception.code, "INVALID_RESPONSE")

    def test_cancel_scope_denial_is_not_retried(self):
        calls = []

        def transport(*args):
            calls.append(args)
            return response({"error": {"code": "TOKEN_SCOPE_REQUIRED"}}, 403)

        client = TldwClient(api_url=API, token="explicit", transport=transport)
        with self.assertRaises(TldwError) as caught:
            client.cancel_job(JOB_ID)
        self.assertEqual(caught.exception.code, "TOKEN_SCOPE_REQUIRED")
        self.assertEqual(len(calls), 1)
