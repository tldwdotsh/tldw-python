# tldw.sh Python client

A synchronous Python 3.10+ client with no runtime dependencies. Install the
versioned wheel from your deployment's `/agents` page:

```sh
python -m pip install /path/to/tldw_client-0.2.0-py3-none-any.whl
```

The intended PyPI distribution name is `tldw-client`, imported as `tldw_client`.
After the owner publishes the first release, `python -m pip install tldw-client`
will install the same client. Until then, use your operator's reviewed wheel.

Use your deployment's API origin and a scoped **tldw API key**, never a model
provider key. Signup and one-time human verification happen in the web app.
REST, MCP and plugins share 500 monthly Free caption/text credits, Hobby/Pro
limits and purchased credits. Polling does not create another analysis or reset your allowance.

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

Other methods:

- `get_job(job_id)`, `get_summary(summary_id)`, `get_usage()` (`account:read`).
- `compare_tiktoks(urls, instructions=..., idempotency_key=...)` returns an
  accepted comparison view. Persist its `id` before waiting.
- `get_comparison(id)`, `wait_for_comparison(id, timeout_seconds=180)` return
  the unwrapped comparison view. Completed output needs `summaries:read`.

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

A standard PEP 517 frontend can also build this dependency-free backend, but no
PyPI release has been made or account ownership established merely by preparing
this repo. The intended `tldw-client` distribution name remains subject to
registry ownership and manual publication approval. CI contains no registry
credentials, automatic publisher or deployment step.

`EXPORT_MANIFEST.json` records canonical-source and export hashes. Synchronize by
reviewing upstream monorepo changes, re-exporting the package and rerunning the
commands above; do not silently diverge or overwrite independent edits.

## Credit controls

Captions, text and existing cached transcripts cost **1 credit per source minute**.
Fresh audio transcription costs **3 credits per source minute** and requires an
eligible paid plan plus explicit `transcription: "if_needed"` (Python:
`transcription="if_needed"`). The default `never` fails with a clear error when
new transcription would be necessary. Supplying `forceAsr: true` /
`force_asr=True` also requires that explicit permission.

Use `maxCredits: "30.00"` in JavaScript or `max_credits="30.00"` in Python to cap
a job. The server validates affordability before new transcription. The cap is
positive, at most 1,000,000 credits, and has at most two decimal places. It is
required whenever `transcription` is `if_needed`. Preserve
these controls in the same idempotent request body on retries. Jobs expose
`credits.estimated`, `reserved`, `final`, the billing class/rate and real source
duration; polling does not create another processing charge.

For `compareTikToks` / `compare_tiktoks`, the cap covers all distinct supplied
clips: each receives an equal share rounded down to 0.01 credit. Unused shares
are not redistributed. Comparison aggregation adds no customer processing credits.
Source minutes round up to 0.1 minute with a 0.5-minute minimum before the rate
multiplier. Raw text uses estimated reading minutes.

Free includes 500 credits per month for captions/text, Hobby costs USD20/month
with 1,000 credits, and Pro costs USD100/month with 5,000 credits. Included
allowance is used first, then purchased credits, which do not expire. Reading
account usage does not buy credits or change plans.
