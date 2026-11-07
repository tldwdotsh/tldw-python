import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient, TldwError

JOB = "00000000-0000-4000-8000-000000000101"
SUMMARY = "00000000-0000-4000-8000-000000000102"
TOKEN = "tldw_local_fixture_not_a_real_secret"


class Handler(BaseHTTPRequestHandler):
    calls: ClassVar[list] = []
    redirect = False
    polls = 0

    def log_message(self, *args):
        pass

    def send_json(self, value, status=200):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.calls.append(("POST", self.path, self.headers.get("Authorization"), data))
        self.send_json(
            {
                "job_id": JOB,
                "job": {"id": JOB, "status": "queued", "progress": 0},
                "status_url": "https://evil.invalid/secret",
            },
            202,
        )

    def do_GET(self):
        self.calls.append(("GET", self.path, self.headers.get("Authorization"), None))
        if self.redirect:
            self.send_response(302)
            self.send_header("Location", "/capture")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self.path.startswith("/v1/jobs/"):
            type(self).polls += 1
            status = "queued" if self.polls == 1 else "completed"
            self.send_json(
                {
                    "id": JOB,
                    "status": status,
                    "progress": 100 if status == "completed" else 10,
                    "summary_id": SUMMARY if status == "completed" else None,
                }
            )
        else:
            self.send_json({"id": SUMMARY, "content": {"answer": "Verified over HTTP"}})


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(
            target=lambda: cls.server.serve_forever(poll_interval=0.01), daemon=True
        )
        cls.thread.start()
        cls.origin = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        Handler.calls = []
        Handler.redirect = False
        Handler.polls = 0

    def test_real_local_transport_submits_polls_and_gets_output(self):
        client = TldwClient(api_url=self.origin, token=TOKEN)
        result = client.analyze(
            "A supplied source",
            idempotency_key="local-transport-1",
            poll_interval_seconds=0.25,
        )
        self.assertEqual(result["summary"]["content"]["answer"], "Verified over HTTP")
        self.assertEqual(
            [call[1] for call in Handler.calls],
            [
                "/v1/analyses",
                f"/v1/jobs/{JOB}",
                f"/v1/jobs/{JOB}",
                f"/v1/summaries/{SUMMARY}",
            ],
        )
        self.assertTrue(all(call[2] == "Bearer " + TOKEN for call in Handler.calls))

    def test_http_redirect_is_never_followed(self):
        Handler.redirect = True
        with self.assertRaises(TldwError) as caught:
            TldwClient(api_url=self.origin, token=TOKEN).get_usage()
        self.assertEqual(caught.exception.status, 302)
        self.assertEqual(len(Handler.calls), 1)
        self.assertEqual(Handler.calls[0][1], "/v1/me")


if __name__ == "__main__":
    unittest.main()
