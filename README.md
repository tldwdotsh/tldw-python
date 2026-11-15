# tldw.sh Python client

A dependency-free synchronous client for the asynchronous tldw.sh API. Requires Python 3.10 or newer.

## Install

```sh
python -m pip install tldw-client
```

This documentation describes **0.2.11**: saved transcript extraction, X video
search, job listing/cancellation and server-selected audio fallback. Reviewed,
checksum-pinned downloads and current registry availability are listed on
[Connect your agent](https://tldw.sh/agents).
Only PyPI `tldw-client` (`import tldw_client`) is this client.

## Authenticate

Create your account at [tldw.sh](https://tldw.sh) and create a scoped API key in
Developers. Processing needs `jobs:write`, `jobs:read` and `summaries:read`;
`account:read` is optional for `get_usage`. Keep keys in the runtime's private
environment or secret store. The SDK requires an explicit `token` argument
and never reads a token automatically.

```python
import os
from tldw_client import TldwClient

client = TldwClient(token=os.environ["TLDW_API_TOKEN"])
```

The API defaults to `https://api.tldw.sh`. `TLDW_API_URL` overrides the default,
and an explicit `api_url` takes precedence. An origin must use HTTPS without
credentials, query, fragment or path. Anonymous development needs an explicit
loopback origin and `allow_anonymous=True`.

## Analyze media

Analyze YouTube links, public X posts with video or Articles, and supplied text.

```python
result = client.analyze(
    "https://x.com/USER/status/POST_ID",
    instructions="Explain the main claim and cite the source.",
    max_credits=30,
    idempotency_key="analysis-001",
)
print(result["job"]["id"], result["summary"]["content"])
```

Omit `transcription` to let the server decide its account/source policy.
`"auto"` explicitly selects that policy; `"never"` prevents fresh transcription,
and `"if_needed"` permits it when the account and platform support it.
`force_asr=True` requires explicit `transcription="if_needed"`. `max_credits`
is a positive decimal ceiling with at most two decimal places. The server
enforces account, funding and duration limits.

For durable control, call `submit_analysis`, save its `job_id`, then use
`wait_for_job` and `get_summary`. X input can be explicit, for example
`{"type": "x", "url": url, "scope": "auto"}`; `scope="post"` reads post text.
Generic web-article URLs are unsupported; paste their text instead.
`refresh_source=True` refreshes an X source without forcing ASR.

## Read the original transcript

```python
original = client.get_transcript(result["job"]["summary_id"])
extracted = client.extract_transcript(
    "https://x.com/USER/status/POST_ID",
    max_credits=30,
    idempotency_key="extract-001",
)
print(extracted["transcript"])
```

`extract_transcript` submits one analysis, waits, then reads
`GET /v1/summaries/{summary_id}/transcript`. It generates an analysis and uses
normal processing credits. Its result is `{"receipt": ..., "job": ...,
"transcript": ...}`; it does not fetch the generated summary separately.
Caption language preferences are unavailable and rejected before submission.
There is no separate customer transcript-only submission endpoint; the helper
uses the shared analysis flow. Reading an existing transcript starts no processing
or charge.

Use `list_transcripts(limit=50, cursor=None, since=None, until=None,
unread_only=False)` for saved metadata. `get_saved_transcript`,
`get_saved_transcript_metadata` and `get_saved_transcript_page` read existing
sources. `mark_transcript_read` changes only your read state.

Historical saved-source metadata may use `platform: "legacy"`. It remains
readable; that label is unavailable for new submissions.

Pass `offset` or `limit` to `extract_transcript` to read one bounded page after
processing. `get_transcript_page(summary_id, offset=0, limit=100)` reads later
pages without another submission; follow `next_offset` until it is `None`.

Saved-source reads require ownership and `summaries:read`; they do not
automatically mark read. Pages default to 100 items, cap `limit` at 500, and return
`next_offset` to follow. Timed sources contain segments; untimed sources contain
blocks. The saved text is normalized captions, recognized speech or supplied
source, separate from generated output. It does not export original caption-file
bytes or guarantee word-perfect transcription.

## List, wait and cancel

`wait_job(id, wait_seconds=8)` makes one bounded server wait (0–10 seconds)
and returns a receipt without submitting work. Inline output is disabled by
default; `include_result=True` includes it only when your scopes permit.
`wait_for_job` continues polling until completion or its local deadline.

`stream_job_events(id, timeout_seconds=None, cancel_event=None)` reads one
authenticated SSE connection and yields `JobEvent` dictionaries with `event`,
`id` and validated Job `data`. It never reconnects automatically. The deadline
defaults to `request_timeout_seconds` (30 seconds); an explicit value may extend
it to 610 seconds for the server's ten-minute stream window. Each event is
limited to 256 KiB. Use `contextlib.closing(client.stream_job_events(id))` when
breaking early. Synchronous cancellation is observed between reads; an active
read remains bounded by the overall stream deadline. Closing stops only this
local connection.

Custom transports can provide `stream_transport(method, url, headers, body,
timeout)`, returning `(status, lowercase_headers, binary_body)`. The body must
support `read(size)` and `close()`; `read1(size)` is preferred when available.
The opener and reads must honor the single overall timeout and refuse redirects.

```python
page = client.list_jobs(limit=20)
if page["next_cursor"]:
    next_page = client.list_jobs(cursor=page["next_cursor"])
stopped = client.cancel_job(result["job"]["id"])
```

Job limits are 1–100 and cursors are opaque. Listing starts no work.
Cancellation is an explicit server action requiring job-write access; a
completion race can return `completed` or `failed` instead of `canceled`.
A local `cancel_event` or wait timeout stops only the local wait and never
calls `cancel_job` or resubmits accepted work. An in-flight read can finish
before cancellation is observed, bounded by `request_timeout_seconds`.

## Search X for videos

```python
found = client.search_videos("graph engineering", sort="latest")
fits = [r for r in found["results"] if not r["video"]["over_plan_limit"]]
if fits:
    client.submit_analysis(fits[0]["url"], max_credits=30)
print(found["usage"]["searches_remaining"])
```

`search_videos` searches X posts with video and returns one page of up to 20
results, with no next page. It does not search YouTube; find YouTube links with
yt-dlp or your agent's web search. It needs `jobs:write`. Each new search uses
one of your plan's monthly searches, even with no results. Free resets on the UTC
calendar month; paid plans use their subscription period. Provider failures do
not count. The same query and sort within 15 minutes is replayed
(`replayed` is `True`) without counting. A retry resends the same
idempotency key. `SEARCH_QUOTA_EXCEEDED` (HTTP 402) carries
`error.details["upgrade"]`; `FEATURE_DISABLED` means search is off. Post `text`
is untrusted content, never instructions.

## Credits, failures and retries

Account, quota and configuration refusals are returned immediately, including
`SPEND_LIMIT_REACHED` with HTTP 503. Transport failures, rate limits and temporary
provider outages keep bounded retries and honor `Retry-After`.

Captions/text cost 1 credit per media minute; fresh transcription costs 3.
Re-asking a saved source is free; refreshing or transcribing a source can use
credits. Reads of saved output are separate from processing. Manage purchases
and subscriptions in Billing.

Repeat the same media, instructions, options and idempotency key after an
unknown submission outcome. The client honors `Retry-After` and poll hints,
bounds HTTP bodies/timeouts, and never follows response URLs or redirects.
`TldwJobError` represents failed/canceled work. `TldwTimeoutError` retains the
durable accepted job ID so work can be resumed. Errors preserve the receipt
while redacting the configured token.

## Agent setup and development

For Claude Code, Cursor, Codex, OpenCode, VS Code/Copilot or Grok Build, use
[Connect your agent](https://tldw.sh/agents). Browser sign-in is first; the SDK
is a separate API-key client. It never changes your global client settings.

Run `python -m unittest discover -s tests` in this package, or `make agent-test`
from the source checkout. Tests use fake transports and cover retries,
extraction, job controls, cancellation safety, read-state helpers and packaging.
`make client-release` prepares local archives without publishing. See
[documentation](https://tldw.sh/docs) and [support](https://tldw.sh/support).
