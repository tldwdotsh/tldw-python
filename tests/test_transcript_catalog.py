import json
import sys
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tldw_client import TldwClient

TRANSCRIPT = "00000000-0000-4000-8000-000000000103"
CONTENT = {
    "transcript_id": TRANSCRIPT,
    "source": "platform_manual",
    "language": "en",
    "timed": True,
    "segments": [{"start_ms": 0, "end_ms": 1000, "text": "Original source."}],
    "blocks": [],
    "text": None,
}


class TranscriptCatalogTests(unittest.TestCase):
    def client(self, calls):
        def transport(method, url, headers, body, timeout):
            parsed = urlsplit(url)
            data = json.loads(body) if body else None
            calls.append((method, parsed, data))
            if method == "PATCH":
                value = {
                    "id": TRANSCRIPT,
                    "unread": not data["read"],
                    "read_at": "2026-01-10T12:00:00Z" if data["read"] else None,
                }
            elif parsed.path.endswith("/metadata"):
                value = {
                    "id": TRANSCRIPT,
                    "video": {"id": TRANSCRIPT, "platform": "legacy"},
                    "unread": True,
                }
            elif parsed.path == "/v1/transcripts":
                value = {"items": [{"id": TRANSCRIPT}], "next_cursor": "next+cursor/=="}
            else:
                value = {
                    **CONTENT,
                    "segments": []
                    if parsed.path.endswith("/page")
                    else CONTENT["segments"],
                    "total_segments": 1,
                    "total_blocks": 0,
                    "next_offset": None,
                }
            return 200, {}, json.dumps(value).encode()

        return TldwClient(
            api_url="https://api.example.test", token="fixture", transport=transport
        )

    def test_wrappers_encode_filters_and_keep_reads_separate_from_marking(self):
        calls = []
        sdk = self.client(calls)
        result = sdk.list_transcripts(
            cursor="next+cursor/==",
            since="2026-01-10T12:00:00+00:00",
            until="2026-01-11T12:00:00Z",
            unread_only=True,
        )
        self.assertEqual(result["items"][0]["id"], TRANSCRIPT)
        query = parse_qs(calls[0][1].query)
        self.assertEqual(query["limit"], ["50"])
        self.assertEqual(query["cursor"], ["next+cursor/=="])
        self.assertEqual(query["since"], ["2026-01-10T12:00:00+00:00"])
        self.assertEqual(query["unread_only"], ["true"])
        self.assertEqual(
            sdk.get_saved_transcript(TRANSCRIPT)["transcript_id"], TRANSCRIPT
        )
        self.assertIsNone(
            sdk.get_saved_transcript_page(TRANSCRIPT, offset=12, limit=25)[
                "next_offset"
            ]
        )
        self.assertEqual(calls[2][1].path, f"/v1/transcripts/{TRANSCRIPT}/page")
        self.assertEqual(
            parse_qs(calls[2][1].query), {"offset": ["12"], "limit": ["25"]}
        )
        self.assertTrue(all(method == "GET" for method, _, _ in calls))
        self.assertFalse(sdk.mark_transcript_read(TRANSCRIPT)["unread"])
        self.assertTrue(sdk.mark_transcript_read(TRANSCRIPT, False)["unread"])
        self.assertEqual(
            [body for _, _, body in calls[3:]], [{"read": True}, {"read": False}]
        )

    def test_invalid_catalog_options_never_send_http(self):
        calls = []
        sdk = self.client(calls)
        for options in (
            {"limit": 101},
            {"limit": True},
            {"unread_only": "true"},
            {"cursor": ""},
            {"since": {}},
        ):
            with self.assertRaises((ValueError, TypeError)):
                sdk.list_transcripts(**options)
        for options in ({"offset": -1}, {"limit": 501}, {"limit": 2.5}):
            with self.assertRaises(ValueError):
                sdk.get_saved_transcript_page(TRANSCRIPT, **options)
        with self.assertRaises(TypeError):
            sdk.mark_transcript_read(TRANSCRIPT, "true")
        with self.assertRaises(ValueError):
            sdk.get_saved_transcript("not-an-id")
        with self.assertRaises(ValueError):
            sdk.get_saved_transcript_metadata("not-an-id")
        self.assertEqual(calls, [])

    def test_metadata_lookup_never_fetches_text_or_marks_read(self):
        calls = []
        result = self.client(calls).get_saved_transcript_metadata(TRANSCRIPT)
        self.assertEqual(result["video"]["platform"], "legacy")
        self.assertEqual(result["id"], TRANSCRIPT)
        self.assertTrue(result["unread"])
        self.assertNotIn("segments", result)
        self.assertEqual(
            [(method, parsed.path) for method, parsed, _ in calls],
            [("GET", f"/v1/transcripts/{TRANSCRIPT}/metadata")],
        )

    def test_metadata_lookup_rejects_mismatched_source_response(self):
        sdk = TldwClient(
            api_url="https://api.example.test",
            token="fixture",
            transport=lambda *args: (
                200,
                {},
                json.dumps(
                    {"id": "other", "video": {"id": TRANSCRIPT}, "unread": True}
                ).encode(),
            ),
        )
        from tldw_client import TldwError

        with self.assertRaises(TldwError) as raised:
            sdk.get_saved_transcript_metadata(TRANSCRIPT)
        self.assertEqual(raised.exception.code, "INVALID_RESPONSE")
