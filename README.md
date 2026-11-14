# tldw.sh Python client

A synchronous Python 3.10+ client with no runtime dependencies.

```sh
python -m pip install tldw-client
```

You can instead install the versioned wheel from your operator's `/agents` page.
Download it, verify it against the SHA-256 checksum shown on that page, then
install the verified local file:

```sh
curl -fsSLo tldw_client-0.2.8-py3-none-any.whl \
  'https://YOUR_APP_ORIGIN/downloads/tldw_client-0.2.8-py3-none-any.whl' &&
  echo 'SHA256_FROM_YOUR_AGENTS_PAGE  tldw_client-0.2.8-py3-none-any.whl' | shasum -a 256 -c - &&
  python -m pip install ./tldw_client-0.2.8-py3-none-any.whl
```

The package is imported as `tldw_client`. Only `tldw-client` on PyPI and your
operator's verified download are this client; similarly named projects are not.

Use your deployment's API origin and a scoped **tldw API key**, never a model
provider key. Signup and one-time human verification happen in the web app.
Polling does not create another analysis or reset your allowance.

The SDK, the Claude connector, Claude Code and the skill use the same tldw.sh
account and credits. A new API key does not add free credits. Free includes 100
monthly caption/text credits and stops at 30 minutes per source.

```python
import os
from tldw_client import TldwClient, TldwTimeoutError

client = TldwClient(
    api_url=os.environ["TLDW_API_URL"], token=os.environ["TLDW_API_TOKEN"]
)
receipt = client.submit_analysis(
    "https://youtu.be/VIDEO_ID",
    instructions="Extract the argument and flag weak evidence.",
    idempotency_key="my-persisted-request-001",
)
print({"job_id": receipt["job_id"]})  # Persist before waiting.
try:
    job = client.wait_for_job(receipt["job_id"], timeout_seconds=180)
    result = client.get_summary(job["summary_id"])
    print(result["content"])
except TldwTimeoutError as error:
    print({"job_id": error.job_id, "status": "resume-polling-later"})
```

`analyze(media, instructions=..., idempotency_key=..., timeout_seconds=180)`
combines submission, polling and result retrieval, returning a dictionary with
`receipt`, `job` and `summary`. A failure after acceptance retains `job_id` and
`receipt`; unknown submission errors retain `idempotency_key`. Reuse that key
with exactly the same source/instructions after an uncertain network outcome.

## Three prompts

Each is one link and one job: submit, keep the job ID, wait, then read.

```python
media = "https://youtu.be/VIDEO_ID"
# YouTube research brief. Cost: 1 credit per caption minute. A 20-minute talk is 20 credits. Reading the saved result again is free.
brief = client.submit_analysis(
    media,
    instructions="Main argument and three useful takeaways. Cite the minute for each.",
    idempotency_key="brief-001",
)
# Transcript only: "Transcript only. Do not summarize." Use submit_transcript; it sends
# no instructions. Cost: Same caption rate, no summary. Reading it again is free.
source = client.submit_transcript(media, idempotency_key="source-001")
# Claims and evidence. Cost: Same caption rate. Audio only on the platforms account_usage lists for your plan, at 3 credits a minute.
claims = client.submit_analysis(
    media,
    instructions="List practical claims, the evidence, and the caveats. Flag anything supported by one example.",
    idempotency_key="claims-001",
)
job = client.wait_for_job(brief["job_id"])
print(client.get_summary(job["summary_id"])["content"])
```

## Limits that stop a job

Four limits can stop a job. `getUsage` / `get_usage` (`GET /v1/me`) returns them
as `walls`, each with `active` for your account. A wall error carries
`error.details.wall` and `error.details.upgrade`, with the next plan and a
`billing_url` for the account owner, at submit time and on a failed job.

- Source length: Free stops at 30 minutes per source; paid plans allow longer
  sources up to their plan limit.
- Monthly credits: when included credits run out, new jobs stop until the next
  billing period or an upgrade. Agents cannot buy credits.
- Audio: a source without captions needs audio transcription, which runs only on
  plans and platforms that allow it (`plan.transcription_platforms`).
- Concurrent jobs: a job over your plan's running-job limit may wait with status
  `queued_for_slot` and a `queue_position`. It is not terminal; the wait helpers
  keep polling until it runs. A full queue returns HTTP 429 `RATE_LIMITED`.

Other methods:

- `submit_transcript(media, ...)`, `get_job(job_id)`, `get_summary(summary_id)`, `get_usage()` (`account:read`).

Polling accepts `on_progress(resource)` and `poll_interval_seconds` (default
1.5, minimum 0.25). It honors larger server polling hints and Retry-After.
Waits have a finite one-hour maximum; each HTTP operation has a separate
30-second budget, configurable through `request_timeout_seconds` (maximum 120).
Pass a `threading.Event` as `cancel_event` to cooperatively stop waiting. The
current bounded HTTP read can finish before cancellation is observed. Stopping
a wait never sends a job cancellation; accepted work remains on the server.

`TldwError` exposes `code`, `status`, `request_id`, `retry_after_seconds` and
durable IDs where known. `TldwJobError` marks failed/canceled work. The SDK never
follows server-supplied URLs or redirects, ignores environment proxies, validates
resource UUIDs and limits API responses to 8 MiB. Transient retries preserve the
same serialized body and idempotency key. Scope, signup and trial denials require
caller action and are not automatically resubmitted.

Credentials must come from your secret manager or private environment, never a
prompt, checked-in file or command argument. The client does not print keys.
Anonymous mode requires explicit loopback development access:
`TldwClient(api_url="http://localhost:8000", allow_anonymous=True)`. It grants no
admin access. Production uses HTTPS and a scoped key.

For maintainers, `python build_wheel.py dist` creates the wheel offline using
only the standard library. The PEP 517 backend supports both source distributions
and wheels with no build dependencies. A standard build frontend can run
`python -m build` to build the source archive and rebuild its wheel in isolation.
Run `python -m unittest discover -s tests` from this source directory to check
transport, polling, billing-safe retries and packaging. Releases are published
from `main` by the monorepo's manual release workflow through PyPI trusted
publishing; a successful local build publishes nothing.

## Standalone development and release preparation

This private repository is an exported snapshot of
[`tldwdotsh/tldw`](https://github.com/tldwdotsh/tldw), directory
`packages/agent-client-python`. The monorepo remains canonical until an independent
release/synchronization workflow is approved and documented. Cloning a private
repository requires organization access and your normal GitHub credential
manager or authorized SSH key; never embed a token in the repository URL.

Run these commands from this repository's root with Python 3.10 or newer:

```sh
python3 -m unittest discover -s tests -v
python3 build_wheel.py dist
python3 -c 'from build_wheel import build_sdist; print(build_sdist("dist"))'
```

All three commands work offline using the standard library. Tests use local HTTP
fixture servers, validate polling and retries, and rebuild the source archive to
verify deterministic wheel bytes. A model key, running SaaS server, monorepo
checkout and build-tool installation are unnecessary. CI tests Python 3.10,
3.12 and 3.14 with read-only repository permissions.

A standard PEP 517 frontend can also build this dependency-free backend. Releases
of `tldw-client` are published from the monorepo's `main` branch through PyPI
trusted publishing; this repository's CI contains no registry credentials,
automatic publisher or deployment step.

`EXPORT_MANIFEST.json` records canonical-source and export hashes. Synchronize by
reviewing upstream monorepo changes, re-exporting the package and rerunning the
commands above; do not silently diverge or overwrite independent edits.

## Extract original source without generated analysis

```python
receipt = client.submit_transcript(
    "https://youtu.be/VIDEO_ID",
    transcription="never",
    max_credits="30.00",
    idempotency_key="original-source-001",
)
print(receipt["job_id"])  # Persist before waiting.
job = client.wait_for_job(receipt["job_id"])
original = client.get_saved_transcript_page(job["transcript_id"], limit=100)
# Follow original["next_offset"] for more source content.
```

`submit_transcript` posts to `POST /v1/transcripts` and always returns a durable
accepted receipt. It never requests a summary or invokes an LLM. Completed
transcript jobs have `output_mode="transcript"`, a `transcript_id`, and no
`summary_id`. `extract_transcript(media, ...)` combines submission, polling and
a full source read, returning `receipt`, `job` and `transcript`; timeout and
source-permission errors retain the accepted receipt and job ID.

`preferred_caption_languages` selects original captions. Spending/ASR controls
and `refresh_source` are supported, but analysis instructions are not.
Submission needs `jobs:write`, polling `jobs:read`, and source reads
`summaries:read`; no account-read scope is needed. New transcript-only jobs
retain normal credit rates and ASR consent/caps. If you already have a saved
transcript ID, read it directly: saved-source reads start no processing job,
invoke no LLM and charge no processing credits.

## Summary and transcript are separate reads

After the job completes, use its `summary_id` to retrieve generated output and
original source independently. Both calls require `summaries:read` and account
ownership. Neither starts another analysis, transcription or processing-credit
charge. The summary response includes transcript metadata, not the full source.

```python
summary = client.get_summary(job["summary_id"])
transcript = client.get_transcript(job["summary_id"])
print(summary["content"]["answer"])
original_text = (
    "\n".join(segment["text"] for segment in transcript["segments"])
    if transcript["timed"]
    else transcript["text"]
)
print(original_text)
```

REST uses `GET /v1/summaries/{id}` and `GET /v1/summaries/{id}/transcript`.
MCP uses `get_summary` and paginated `get_transcript`. Timed YouTube/TikTok
sources expose `segments`; text/X sources expose `blocks` and `text`. X video
caption blocks retain `start_s` / `end_s` alongside separate untimed post context.
The saved source stays in its original language when requesting a translated summary.

## Saved transcription inbox

`list_transcripts` returns the latest 50 distinct sources requested by your account,
including sources saved before an analysis failed. It reads metadata only and does
not mark anything read. All library, source and read-state calls require `summaries:read`.

```python
page = client.list_transcripts(limit=50, unread_only=True)
for item in page["items"]:
    print(item["id"], item["video"]["title"], item["last_requested_at"])
    source = client.get_saved_transcript_page(item["id"], offset=0, limit=100)
    # Follow source["next_offset"] for additional content. Mark only when ready.
    client.mark_transcript_read(item["id"], read=True)
```

Pass `cursor=page["next_cursor"]` with the same filters for older entries. `since`
is inclusive and `until` exclusive, using ISO 8601 timestamps against
`last_requested_at`. `read=False` marks a source unread again. Reading source
content through REST/MCP/SDK never implicitly changes its read state.
Use `get_saved_transcript_metadata(id)` to read one source’s title, dates and
unread status directly, including sources outside the current catalog page. This
GET does not load the transcript body or start a processing job.

X posts, X video captions and articles reuse an owned source snapshot for 24 hours.
Use `submit_analysis(..., refresh_source=True)` to retrieve current source content.
YouTube/TikTok keep reusing saved transcripts; another brief does not redownload
audio or repeat transcription when a usable source already exists.

## X video captions and output language

Pass an X post URL directly. Default `scope=auto` detects one attached video and
analyzes its available captions plus separately labeled post context when the
deployment enables X video extraction. You do not need a subtitle URL or a
separate provider key. Use `{"type":"x","url":"YOUR_X_POST_URL","scope":"post"}`
only for post text alone; thread scope does not recursively analyze its videos.

```python
receipt = client.submit_analysis(
    "https://x.com/hamptonism/status/2071631335836225751",
    instructions="Summarize the spoken content in Spanish. Keep it concise and avoid repetition.",
    transcription="never",
    idempotency_key="x-video-spanish-001",
)
print({"job_id": receipt["job_id"]})  # Persist before waiting.
job = client.wait_for_job(
    receipt["job_id"],
    on_progress=lambda state: print(
        {"status": state["status"], "progress": state["progress"]}
    ),
)
result = client.get_summary(job["summary_id"])
print(result["content"]["answer"])
```

The response is a durable job receipt. Save its ID before waiting; a timeout does
not cancel the job. Use a fresh stable key for a different brief or language and
reuse the exact body/key after an uncertain submission. Without instructions,
the service produces a concise summary. A translated summary is generated text
inside the same schema-3 result, not dubbed audio, a translated video or an SRT
translation; the original captions remain saved separately.

Captioned X videos use actual video duration at 1 credit/minute under normal
rounding (128.197 seconds = 2.2 credits), not the post's reading time. Missing
captions fail explicitly with no customer processing charge; X audio
transcription and visual analysis are not enabled, even with `if_needed`.

## Credit controls

Captions, text and existing cached transcripts cost **1 credit per source minute**.
Fresh audio transcription costs **3 credits per source minute** and requires an
eligible paid plan or server-confirmed billing exemption, plus explicit `transcription: "if_needed"` (Python:
`transcription="if_needed"`). The default `never` fails with a clear error when
new transcription would be necessary. Supplying `forceAsr: true` /
`force_asr=True` also requires that explicit permission.

Use `maxCredits: "30.00"` in JavaScript or `max_credits="30.00"` in Python to cap
a job. The server validates affordability before new transcription. The cap is
positive, at most 1,000,000 credits, and has at most two decimal places. It is
required for ordinary paid accounts when `transcription` is `if_needed`. Preserve
these controls in the same idempotent request body on retries. Jobs expose
`credits.estimated`, `reserved`, `final`, the billing class/rate and real source
duration; polling does not create another processing charge.

Source minutes round up to 0.1 minute with a 0.5-minute minimum before the rate
multiplier. Raw text uses estimated reading minutes.

Free includes 100 credits per month for captions/text, Hobby costs USD20/month
with 1,000 credits, and Pro costs USD99/month with 5,000 credits. Included
credits reset monthly. Agents cannot buy credits, and reading account usage does
not change plans.

Server-confirmed billing exemption: the authenticated server decides whether an account is exempt. Exempt accounts may omit a credit ceiling and do not incur customer-credit charges; explicit transcription permission and resource/provider limits still apply. Ordinary paid accounts allowing fresh transcription must supply a ceiling or receive `TRANSCRIPTION_NOT_ALLOWED` (HTTP 422); `MAX_CREDITS_EXCEEDED` (HTTP 422) means a supplied ceiling is invalid or below the required charge. Clients validate supplied values, then pass requests through without fetching extra account scopes or inferring privileges from names, roles, or key prefixes.

## Changes in 0.2.8

Customer subtitle uploads are no longer supported. `submit_transcript()` and
`extract_transcript()` no longer accept `upload_id` and always need `media`.

## Changes in 0.2.7

API keys can reach only the customer API, so the client now calls only those
routes. Removed:

- `compare_tiktoks()` (`POST /v1/comparisons`)
- `get_comparison()` (`GET /v1/comparisons/{id}`)
- `wait_for_comparison()` (polled `GET /v1/comparisons/{id}`)

`TldwError.comparison_id` remains for compatibility and is always `None`.
