"""Stream parsing/cleanup regressions using in-memory responses only."""

import json
import sys
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tldw_client
from tldw_client import TldwCanceledError, TldwClient, TldwError, TldwTimeoutError

JOB = "00000000-0000-4000-8000-000000000101"
SUMMARY = "00000000-0000-4000-8000-000000000102"
OTHER = "00000000-0000-4000-8000-000000000103"
TOKEN = "tldw_stream_fixture_not_a_real_secret"


def job(status="queued", **extra):
    return {
        "id": JOB,
        "status": status,
        "progress": 100 if status == "completed" else 20,
        "summary_id": SUMMARY if status == "completed" else None,
        **extra,
    }


def frame(kind="snapshot", value=None, event_id=None, newline="\n"):
    value = job() if value is None else value
    prefix = f"id: {event_id}{newline}" if event_id is not None else ""
    return (
        prefix
        + f"event: {kind}{newline}data: "
        + json.dumps(value, ensure_ascii=False)
        + newline * 2
    ).encode()


class Stream:
    def __init__(self, content=b"", *, chunk_size=4096, on_read=None):
        self.content = content
        self.chunk_size = chunk_size
        self.on_read = on_read
        self.closed = False
        self.close_count = 0

    def read1(self, size):
        if self.closed:
            raise AssertionError("read after close")
        if self.on_read:
            self.on_read()
        count = min(size, self.chunk_size)
        result, self.content = self.content[:count], self.content[count:]
        return result

    read = read1

    def close(self):
        self.closed = True
        self.close_count += 1


class JobEventsTests(unittest.TestCase):
    def client(self, body, *, status=200, headers=None, **options):
        self.calls = []
        self.now = 0

        def transport(*args):
            self.calls.append(args)
            return (
                status,
                {"content-type": "text/event-stream", **(headers or {})},
                body,
            )

        return TldwClient(
            api_url="https://api.example.test",
            token=TOKEN,
            transport=lambda *args: self.fail("stream used buffered transport"),
            stream_transport=transport,
            _clock=lambda: self.now,
            **options,
        )

    def test_single_owned_get_parses_fragmented_utf8_crlf_comments_and_terminal(self):
        body = Stream(
            b"\xef\xbb\xbf: ready\r\n\r\n"
            + frame(value=job(stage_detail="🧠 saved source"), newline="\r\n")
            + b": ping\r\n\r\n"
            + frame("status", job("summarizing"), event_id=12, newline="\r")
            + frame("completed", job("completed"), event_id=13)
            + b"unread after terminal",
            chunk_size=1,
        )
        events = self.client(body).stream_job_events(JOB.upper())
        first = next(events)
        self.assertEqual(first["event"], "snapshot")
        self.assertIsNone(first["id"])
        self.assertEqual(first["data"]["stage_detail"], "🧠 saved source")
        self.assertEqual(next(events)["id"], "12")
        terminal = next(events)
        self.assertEqual(terminal["data"]["status"], "completed")
        self.assertTrue(body.closed, "terminal event must release body before yield")
        self.assertEqual(list(events), [])
        self.assertEqual(body.close_count, 1)
        self.assertEqual(len(self.calls), 1)
        method, url, headers, payload, timeout = self.calls[0]
        self.assertEqual(method, "GET")
        self.assertEqual(url, f"https://api.example.test/v1/jobs/{JOB}/events")
        self.assertEqual(
            headers, {"Accept": "text/event-stream", "Authorization": "Bearer " + TOKEN}
        )
        self.assertIsNone(payload)
        self.assertEqual(timeout, 30)

    def test_multiline_data_and_ignored_fields(self):
        payload = json.dumps(job(), indent=2)
        body = Stream(
            (
                "retry: 5000\nunknown: ignored\nevent: snapshot\n"
                + "".join(f"data: {line}\n" for line in payload.splitlines())
                + "\n"
            ).encode()
        )
        events = list(self.client(body).stream_job_events(JOB))
        self.assertEqual(events[0]["data"], job())
        self.assertTrue(body.closed)
        self.assertEqual(len(self.calls), 1)

    def test_completed_snapshot_closes_without_reading_following_frames(self):
        body = Stream(frame(value=job("completed")), on_read=None)
        events = self.client(body).stream_job_events(JOB)
        self.assertEqual(next(events)["event"], "snapshot")
        self.assertTrue(body.closed)
        events.close()
        self.assertEqual(body.close_count, 1)

    def test_clean_disconnect_ends_without_reconnect_or_new_work(self):
        body = Stream(frame())
        self.assertEqual(len(list(self.client(body).stream_job_events(JOB))), 1)
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(body.closed)

    def test_generator_close_releases_nonterminal_connection(self):
        body = Stream(frame() + frame("status", job("summarizing")), chunk_size=1)
        events = self.client(body).stream_job_events(JOB)
        next(events)
        events.close()
        self.assertTrue(body.closed)
        self.assertEqual(body.close_count, 1)

    def test_cancel_before_connect_and_after_blocked_read(self):
        cancel = Event()
        cancel.set()
        body = Stream(frame())
        client = self.client(body)
        with self.assertRaises(TldwCanceledError) as caught:
            next(client.stream_job_events(JOB, cancel_event=cancel))
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(self.calls, [])
        cancel.clear()
        body.on_read = cancel.set
        with self.assertRaises(TldwCanceledError) as caught:
            next(client.stream_job_events(JOB, cancel_event=cancel))
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertTrue(body.closed)
        self.assertEqual(len(self.calls), 1)

    def test_absolute_deadline_is_checked_after_read_and_closes(self):
        body = Stream(frame())
        client = self.client(body)
        body.on_read = lambda: setattr(self, "now", 2)
        with self.assertRaises(TldwTimeoutError) as caught:
            next(client.stream_job_events(JOB, timeout_seconds=1))
        self.assertEqual(caught.exception.job_id, JOB)
        self.assertEqual(caught.exception.status, 200)
        self.assertTrue(body.closed)
        self.assertEqual(len(self.calls), 1)

    def test_transport_and_read_failures_never_retry(self):
        for error in (URLError("failed"), OSError("failed"), TimeoutError()):
            with self.subTest(error=error):
                body = Stream()
                client = self.client(body)

                def fail(error=error):
                    raise error

                body.on_read = fail
                expected = (
                    TldwTimeoutError if isinstance(error, TimeoutError) else TldwError
                )
                with self.assertRaises(expected) as caught:
                    next(client.stream_job_events(JOB))
                self.assertEqual(caught.exception.job_id, JOB)
                self.assertTrue(body.closed)
                self.assertEqual(len(self.calls), 1)

    def test_api_error_keeps_code_status_retry_hint_and_redacts_token(self):
        body = Stream(
            json.dumps(
                {
                    "error": {
                        "code": "SPEND_LIMIT_REACHED",
                        "message": "limit " + TOKEN,
                        "details": {"secret": TOKEN},
                    }
                }
            ).encode()
        )
        client = self.client(
            body, status=503, headers={"retry-after": "12", "x-request-id": "request-1"}
        )
        with self.assertRaises(TldwError) as caught:
            next(client.stream_job_events(JOB))
        error = caught.exception
        self.assertEqual(
            (error.code, error.status, error.job_id), ("SPEND_LIMIT_REACHED", 503, JOB)
        )
        self.assertEqual(error.retry_after_seconds, 12)
        self.assertEqual(error.request_id, "request-1")
        self.assertNotIn(TOKEN, str(error) + repr(error.details))
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(body.closed)

    def test_redirect_is_an_error_and_returned_urls_are_not_followed(self):
        body = Stream()
        client = self.client(
            body, status=302, headers={"location": "https://evil.invalid/steal"}
        )
        with self.assertRaises(TldwError) as caught:
            next(client.stream_job_events(JOB))
        self.assertEqual(
            (caught.exception.code, caught.exception.status), ("HTTP_ERROR", 302)
        )
        self.assertEqual(len(self.calls), 1)
        body = Stream(
            frame(
                value=job(
                    "completed",
                    events_url="https://evil.invalid/steal",
                    result_url="https://evil.invalid/steal",
                )
            )
        )
        self.assertEqual(len(list(self.client(body).stream_job_events(JOB))), 1)
        self.assertEqual(len(self.calls), 1)

    def test_invalid_framing_payload_identity_and_terminal_consistency_close(self):
        invalid = [
            b"event: snapshot\ndata: \xff\n\n",
            b"event: snapshot\ndata: not-json\n\n",
            b"event: snapshot\ndata: {}\n\n",
            b"event: snapshot\ndata: {}",
            b"event: snapshot\ndata: " + b"[" * 2000 + b"0" + b"]" * 2000 + b"\n\n",
            frame(value=job(id=OTHER)),
            frame(value=job(job_id=OTHER)),
            frame(value=job(progress=True)),
            frame(value=job(status="invented")),
            frame(value=job("completed", summary_id="https://evil.invalid/steal")),
            frame("status", job("completed")),
            frame("failed", job("summarizing")),
            frame("unexpected"),
            frame(event_id="0"),
            frame(event_id="bad"),
            frame(event_id="1" * 65),
        ]
        for content in invalid:
            with self.subTest(content=content[:100]):
                body = Stream(content)
                with self.assertRaises(TldwError) as caught:
                    list(self.client(body).stream_job_events(JOB))
                self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
                self.assertEqual(caught.exception.job_id, JOB)
                self.assertEqual(caught.exception.status, 200)
                self.assertTrue(body.closed)

    def test_deep_error_json_preserves_http_failure_without_reconnecting(self):
        body = Stream(b"[" * 2000 + b"0" + b"]" * 2000)
        client = self.client(body, status=503)
        with self.assertRaises(TldwError) as caught:
            next(client.stream_job_events(JOB))
        self.assertEqual(
            (caught.exception.code, caught.exception.status), ("HTTP_ERROR", 503)
        )
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(body.closed)

    def test_line_and_multiline_event_limits_are_independent(self):
        for content in (
            b":" + b"x" * (tldw_client._MAX_EVENT_BYTES + 1),
            b"event: snapshot\n" + (b"data: " + b"x" * 1024 + b"\n") * 257,
        ):
            with self.subTest(length=len(content)):
                body = Stream(content)
                with self.assertRaises(TldwError) as caught:
                    list(self.client(body).stream_job_events(JOB))
                self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
                self.assertTrue(body.closed)

    def test_content_type_arguments_and_configured_origin(self):
        body = Stream(frame())
        client = self.client(body, headers={"content-type": "application/json"})
        with self.assertRaises(TldwError) as caught:
            next(client.stream_job_events(JOB))
        self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
        self.assertTrue(body.closed)
        self.calls.clear()
        for value in ("https://evil.invalid/steal", "../other", None):
            with self.assertRaises(ValueError):
                client.stream_job_events(value)
        for timeout in (True, 0, float("inf"), 611):
            with self.assertRaises(ValueError):
                client.stream_job_events(JOB, timeout_seconds=timeout)
        self.assertEqual(self.calls, [])

    def test_default_stream_opener_ignores_environment_proxy_and_redirect(self):
        body = Stream(frame(value=job("completed")))
        body.status = 200
        body.headers = {"Content-Type": "text/event-stream"}
        with patch.object(tldw_client, "build_opener") as build:
            build.return_value.open.return_value = body
            client = TldwClient(api_url="https://api.example.test", token=TOKEN)
            self.assertEqual(len(list(client.stream_job_events(JOB))), 1)
        handlers = build.call_args.args
        self.assertTrue(
            any(isinstance(handler, tldw_client._NoRedirect) for handler in handlers)
        )
        proxies = [
            handler
            for handler in handlers
            if isinstance(handler, tldw_client.ProxyHandler)
        ]
        self.assertEqual(len(proxies), 1)
        self.assertEqual(proxies[0].proxies, {})
        request = build.return_value.open.call_args.args[0]
        self.assertEqual(
            request.full_url, f"https://api.example.test/v1/jobs/{JOB}/events"
        )
        self.assertEqual(request.get_header("Authorization"), "Bearer " + TOKEN)
        self.assertTrue(body.closed)


if __name__ == "__main__":
    unittest.main()
