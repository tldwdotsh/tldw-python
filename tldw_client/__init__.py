"""A dependency-free, scoped-key client for the tldw.sh async API.

No server-returned URLs are followed. Waiting never cancels accepted server work.

tldw.sh summarizes and analyzes videos and other media: YouTube links,
public X posts (with an attached video or Article) and pasted text.
Processing is asynchronous. A submission is accepted at once with a durable
job ID, the job is polled until it finishes, and the result is read
separately::

    import os
    from tldw_client import TldwClient

    client = TldwClient(token=os.environ["TLDW_API_TOKEN"])

    # One call: submit, wait, then read the summary.
    result = client.analyze(url, max_credits=30)
    print(result["summary"]["content"])

    # Or keep durable control. Save receipt["job_id"]: after an interruption,
    # resume with wait_for_job instead of submitting again.
    receipt = client.submit_analysis(url, idempotency_key="request-001")
    job = client.wait_for_job(receipt["job_id"], timeout_seconds=600)
    summary = client.get_summary(job["summary_id"])

The client is synchronous and uses only the standard library (Python 3.10+).
Guarantees shared by every method:

* Only the configured API origin is contacted. Request paths are built from
  validated UUIDs, redirects are not followed, proxy settings from the
  environment are ignored, and URLs in responses are never fetched.
* The token is kept in a private attribute, sent only as a Bearer header and
  redacted from error messages and attached error details.
* Buffered responses are capped at 8 MiB and must be JSON objects of the
  documented shape. Streamed event lines and frames are capped at 256 KiB;
  malformed responses raise TldwError with code ``INVALID_RESPONSE``.
* Retries are bounded (at most 4 attempts) and cover only transient statuses
  (429, 502, 503, 504) and network failures. Account, quota and configuration
  error codes are final even with those statuses. A POST is retried only with the
  same body and ``Idempotency-Key``, so the server replays the original
  result instead of creating a second job. Event streams use one connection
  without retrying or reconnecting.
* With the default transport, each request has one overall deadline covering
  connection setup, TLS, sending and reading, not just individual socket
  operations. (A slow DNS lookup cannot be interrupted, but the deadline is
  checked as soon as it returns.)

Invalid arguments raise ValueError or TypeError before any request is sent.
API, network and response problems raise TldwError or one of its subclasses.
"""

from __future__ import annotations

import io
import json
import math
import os
import socket
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from typing import Any, Literal, TypedDict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import (
    HTTPHandler,
    HTTPRedirectHandler,
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
)

__all__ = [
    "DEFAULT_API_URL",
    "JobEvent",
    "TldwCanceledError",
    "TldwClient",
    "TldwError",
    "TldwJobError",
    "TldwTimeoutError",
]
# Must match pyproject.toml and build_wheel.py; the wheel build checks this.
__version__ = "0.2.13"
# Production API origin, used when neither api_url nor TLDW_API_URL is set.
DEFAULT_API_URL = "https://api.tldw.sh"
# Largest response body the client accepts.
_MAX_BODY = 8 * 1024 * 1024
# SSE is read incrementally; a hostile peer cannot grow a line or event forever.
_MAX_EVENT_BYTES = 256 * 1024
_STREAM_CHUNK_BYTES = 4096
# Rate limiting and temporary gateway/upstream failures: retried with backoff.
_TRANSIENT = {429, 502, 503, 504}
_NON_RETRYABLE_CODES = {
    "SPEND_LIMIT_REACHED",
    "ACCOUNT_UNAVAILABLE",
    "UNAUTHENTICATED",
    "FORBIDDEN",
    "SESSION_REQUIRED",
    "INSUFFICIENT_SCOPE",
    "TRIAL_PAUSED",
    "TRIAL_ACTIVATION_REQUIRED",
    "QUOTA_EXCEEDED",
    "SEARCH_QUOTA_EXCEEDED",
    "VIDEO_TOO_LONG",
    "ASR_REQUIRES_UPGRADE",
    "PLAN_FEATURE_REQUIRED",
    "MAX_CREDITS_EXCEEDED",
    "DURATION_LIMIT_EXCEEDED",
    "TRANSCRIPTION_NOT_ALLOWED",
    "CAPTIONS_UNAVAILABLE",
    "TRANSCRIPTION_NOT_CONFIGURED",
    "FEATURE_DISABLED",
    "SOURCE_NOT_CONFIGURED",
    "BILLING_CONFIGURATION_ERROR",
}


def _spending_options(transcription=None, max_credits=None, force_asr=None):
    """Validate cost and transcription options and map them to body fields.

    Omitted values are left out so the server applies its current policy.
    ``max_credits`` (a positive decimal up to 1,000,000 with at most two
    decimal places) is sent as a two-decimal string, for example ``"30.00"``,
    so no float rounding reaches the server. Processing uses captions or saved
    source only. Legacy ``transcription`` and ``force_asr`` values are accepted
    but ignored by the server. Raises ValueError for invalid values.
    """
    if transcription is not None and transcription not in (
        "auto",
        "never",
        "if_needed",
    ):
        raise ValueError("transcription must be auto, never or if_needed.")
    if force_asr is not None and not isinstance(force_asr, bool):
        raise ValueError("force_asr must be a boolean.")
    result = {}
    if transcription is not None:
        result["transcription"] = transcription
    if force_asr is not None:
        result["force_asr"] = force_asr
    if max_credits is not None:
        try:
            # bool is an int subclass; True must not become a 1-credit cap.
            if isinstance(max_credits, bool):
                raise TypeError
            # str() first so a float such as 0.1 becomes Decimal("0.1"), not
            # its binary expansion.
            value = Decimal(str(max_credits))
            if (
                not value.is_finite()
                or not 0 < value <= 1_000_000
                or value != value.quantize(Decimal("0.01"))
            ):
                raise ValueError
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError(
                "max_credits must be greater than zero, at most 1000000, with at most two decimal places."
            ) from None
        result["max_credits"] = format(value.quantize(Decimal("0.01")), "f")
    return result


# Every job status the API reports; any other value is an invalid response.
# completed, failed and canceled are terminal; the rest mean "keep polling".
_JOB_STATES = {
    "queued",
    # Waiting for one of the plan's concurrent-job slots; not terminal.
    "queued_for_slot",
    "fetching_metadata",
    "fetching_transcript",
    "downloading_audio",
    "transcribing",
    "summarizing",
    "completed",
    "failed",
    "canceled",
}


class JobEvent(TypedDict):
    """One validated job SSE frame; ``id`` is absent from server snapshots."""

    event: Literal["snapshot", "status", "completed", "failed", "canceled"]
    id: str | None
    data: dict[str, Any]


class TldwError(Exception):
    """Base class for errors about the API, the network or a response.

    Invalid arguments raise ValueError or TypeError instead.

    Attributes:
        code: Machine-readable code. API errors carry the server's code, for
            example ``QUOTA_EXCEEDED``, ``RATE_LIMITED`` or
            ``TOKEN_SCOPE_REQUIRED``. Client-side codes include
            ``CLIENT_ERROR``, ``HTTP_ERROR``, ``INVALID_RESPONSE``,
            ``NETWORK_ERROR``, ``WAIT_TIMEOUT``, ``WAIT_CANCELED``,
            ``JOB_FAILED`` and ``CANCELED``.
        status: HTTP status, or None when no response was received.
        request_id: The server's request ID, useful when contacting support.
        retry_after_seconds: The parsed ``Retry-After`` header, or None.
        job_id: The job this error concerns: the job being submitted or
            waited on, or an existing job named in ``details["job_id"]`` (for
            example with ``DUPLICATE_ACTIVE_JOB``).
        comparison_id: Retired; always None.
        details: The API's ``error.details`` with the token redacted, for
            example ``{"required_scope": ...}`` or ``{"upgrade": {...}}``.

    Submissions and searches also set ``idempotency_key`` on their errors
    (replay with it when the outcome is unknown), and ``analyze`` and
    ``extract_transcript`` set ``receipt`` once the job was accepted.
    """

    def __init__(
        self,
        message,
        *,
        code="CLIENT_ERROR",
        status=None,
        request_id=None,
        retry_after_seconds=None,
        job_id=None,
        comparison_id=None,
        details=None,
    ):
        super().__init__(message)
        self.code = code
        self.status = status
        self.request_id = request_id
        self.retry_after_seconds = retry_after_seconds
        self.job_id = job_id
        self.comparison_id = comparison_id
        # Redacted API error.details, e.g. {"job_id": ...} or {"required_scope": ...}.
        self.details = details


class TldwTimeoutError(TldwError):
    """A local deadline passed (code ``WAIT_TIMEOUT``).

    Raised when a wait's ``timeout_seconds`` or a request's
    ``request_timeout_seconds`` runs out. Work the server already accepted
    keeps running: resume it with ``wait_for_job(error.job_id)``. For a
    submission whose outcome is unknown, replay it with
    ``error.idempotency_key``.
    """

    def __init__(
        self,
        message="The wait ended. Accepted work continues on the server.",
        **details,
    ):
        super().__init__(message, code="WAIT_TIMEOUT", **details)


class TldwCanceledError(TldwError):
    """The caller set ``cancel_event`` (code ``WAIT_CANCELED``).

    Only the client stopped; work the server already accepted continues.
    Call ``cancel_job`` to stop a job.
    """

    def __init__(self):
        super().__init__(
            "The wait was canceled. Accepted work continues on the server.",
            code="WAIT_CANCELED",
        )


class TldwJobError(TldwError):
    """A job ended as ``failed`` or ``canceled``.

    The message and code come from the job's ``error`` object when present;
    otherwise the code is ``CANCELED`` or ``JOB_FAILED``. ``resource`` holds
    the final job with the token redacted. ``kind`` is always ``"job"``; it
    selects which ``<kind>_id`` attribute receives the resource ID.
    """

    def __init__(self, resource, kind="job"):
        error = resource.get("error") or {}
        error = error if isinstance(error, dict) else {}
        super().__init__(
            error.get("message")
            if isinstance(error.get("message"), str)
            else f"The {kind} {resource['status']}.",
            code=error.get("code")
            if isinstance(error.get("code"), str)
            else ("CANCELED" if resource["status"] == "canceled" else "JOB_FAILED"),
            **{f"{kind}_id": resource["id"]},
        )
        self.resource = resource


def _identifier(value, label="ID"):
    """Return a canonical 36-character UUID string, lowercased.

    Request paths are built only from IDs validated here, so no input can
    change a request's path or host. Raises ValueError otherwise.
    """
    try:
        if not isinstance(value, str) or len(value) != 36:
            raise ValueError()
        parsed = uuid.UUID(value)
        # uuid.UUID also accepts braces, "urn:uuid:" and missing hyphens;
        # only the canonical 8-4-4-4-12 form round-trips unchanged.
        if str(parsed) != value.lower():
            raise ValueError()
        return str(parsed)
    except (ValueError, AttributeError):
        raise ValueError(f"{label} must be a UUID.") from None


def _bound(value, label, minimum, maximum):
    """Return a finite int or float (not bool) within the bounds, as float."""
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{label} must be between {minimum} and {maximum}.")
    return float(value)


def _object(value, label):
    """Require a JSON object where the API contract promises one."""
    if not isinstance(value, dict):
        raise TldwError(f"Invalid {label} response.", code="INVALID_RESPONSE")
    return value


def _idempotency_key(value):
    """Return the caller's idempotency key, or a new random UUID.

    Keys are 1-128 printable ASCII characters without whitespace, as the
    ``Idempotency-Key`` header requires.
    """
    value = str(uuid.uuid4()) if value is None else value
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or not all(33 <= ord(char) <= 126 for char in value)
    ):
        raise ValueError(
            "idempotency_key must contain 1–128 printable ASCII characters without whitespace."
        )
    return value


def _page_options(offset, limit):
    for name, value, minimum, maximum in (
        ("offset", offset, 0, 1_000_000_000),
        ("limit", limit, 1, 500),
    ):
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(
                f"{name} must be an integer between {minimum} and {maximum}."
            )


def _retry_delay(value):
    """Parse ``Retry-After`` (delta-seconds or an HTTP-date) into seconds.

    The result is capped at one hour; None when absent or unparseable.
    """
    if not value:
        return None
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return min(3600, seconds)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return min(
                3600, max(0, (date - datetime.now(timezone.utc)).total_seconds())
            )
        except (ValueError, TypeError, OverflowError):
            return None


def _safe_error_resource(resource, token):
    """Deep-copy a server value with the token replaced by ``[REDACTED]``.

    Applies to dictionary keys and strings at any depth, so errors (which
    callers tend to log) never leak the credential even if the server echoes
    it. Depth is capped to bound the work done on hostile input.
    """
    if not token:
        return resource

    def redact(value, depth=0):
        if depth > 100:
            return "[OMITTED]"
        if isinstance(value, str):
            return value.replace(token, "[REDACTED]")
        if isinstance(value, list):
            return [redact(item, depth + 1) for item in value]
        if isinstance(value, dict):
            return {
                str(field).replace(token, "[REDACTED]"): redact(item, depth + 1)
                for field, item in value.items()
            }
        return value

    return redact(resource)


class _NoRedirect(HTTPRedirectHandler):
    """Never follow redirects.

    Returning None makes urllib raise HTTPError for the 3xx response, which
    ``_http_transport`` returns as an ordinary (non-2xx) response.
    """

    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def _remaining(deadline):
    """Seconds left before a ``time.monotonic()`` deadline.

    Raises TimeoutError once it has passed.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError()
    return remaining


class _DeadlineReader(io.RawIOBase):
    """Refresh the socket budget before every read, including HTTP framing.

    http.client reads the status line, headers, chunk framing and body through
    this file object, so each read gets a socket timeout equal to the time
    left and the whole response shares one deadline.
    """

    def __init__(self, raw, connection, deadline):
        self._raw = raw
        self._connection = connection
        self._deadline = deadline

    def readable(self):
        return True

    def readinto(self, buffer):
        self._connection.settimeout(_remaining(self._deadline))
        count = self._raw.readinto(buffer)
        _remaining(self._deadline)
        return count

    def close(self):
        try:
            self._raw.close()
        finally:
            super().close()


class _DeadlineSocket:
    """Adapt http.client's socket/file interface without background threads.

    Wraps a connected plain or TLS socket. Attribute access is delegated to
    the real socket; sends and the file object used for reading are bounded
    by the deadline.
    """

    def __init__(self, connection, deadline):
        self._connection = connection
        self._deadline = deadline

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def gettimeout(self):
        # ssl.wrap_socket inherits this remaining budget for its handshake.
        return _remaining(self._deadline)

    def sendall(self, data):
        self._connection.settimeout(_remaining(self._deadline))
        self._connection.sendall(data)
        _remaining(self._deadline)

    def makefile(self, mode):
        raw = self._connection.makefile(mode, buffering=0)
        return io.BufferedReader(_DeadlineReader(raw, self._connection, self._deadline))


class _DeadlineConnection:
    """Mixin enforcing one deadline on an http.client connection.

    Covers DNS lookup (checked afterwards), connect, TLS handshake, send and
    read. Combined with HTTPConnection and HTTPSConnection below; the
    ``deadline`` keyword is a ``time.monotonic()`` timestamp.
    """

    def __init__(self, *args, deadline, **kwargs):
        self._deadline = deadline
        super().__init__(*args, **kwargs)
        # HTTPConnection.connect() opens its socket through this attribute.
        self._create_connection = self._connect_socket

    def _connect_socket(self, address, timeout, source_address):
        # The system resolver is synchronous and has no portable cancellation
        # API. Check elapsed time immediately afterwards, before opening a socket.
        addresses = socket.getaddrinfo(*address, type=socket.SOCK_STREAM)
        _remaining(self._deadline)
        last_error = None
        # Try each resolved address in turn, like socket.create_connection,
        # with whatever budget remains.
        for family, kind, protocol, _, destination in addresses:
            connection = socket.socket(family, kind, protocol)
            try:
                connection.settimeout(_remaining(self._deadline))
                if source_address:
                    connection.bind(source_address)
                connection.connect(destination)
                _remaining(self._deadline)
                return _DeadlineSocket(connection, self._deadline)
            except OSError as error:
                connection.close()
                last_error = error
        if last_error is not None:
            raise last_error
        raise OSError("The API host did not resolve to a stream address.")

    def connect(self):
        super().connect()
        # HTTPSConnection replaces the initial socket during TLS negotiation.
        if not isinstance(self.sock, _DeadlineSocket):
            self.sock = _DeadlineSocket(self.sock, self._deadline)
        _remaining(self._deadline)


class _DeadlineHTTPConnection(_DeadlineConnection, HTTPConnection):
    pass


class _DeadlineHTTPSConnection(_DeadlineConnection, HTTPSConnection):
    pass


class _DeadlineHTTPHandler(HTTPHandler):
    """urllib handler that opens deadline-bound HTTP connections."""

    def __init__(self, deadline):
        super().__init__()
        self._deadline = deadline

    def http_open(self, request):
        return self.do_open(
            lambda *args, **kwargs: _DeadlineHTTPConnection(
                *args, deadline=self._deadline, **kwargs
            ),
            request,
        )


class _DeadlineHTTPSHandler(HTTPSHandler):
    """urllib handler that opens deadline-bound HTTPS connections.

    TLS uses the handler's default context, so certificates and host names
    are verified as usual.
    """

    def __init__(self, deadline):
        super().__init__()
        self._deadline = deadline

    def https_open(self, request):
        return self.do_open(
            lambda *args, **kwargs: _DeadlineHTTPSConnection(
                *args, deadline=self._deadline, **kwargs
            ),
            request,
            context=self._context,
        )


def _http_stream_transport(method, url, headers, payload, timeout):
    """Open one response without following redirects or environment proxies.

    Returns ``(status, lower_case_headers, binary_body)``. The caller owns
    the body and must close it. A custom ``stream_transport`` takes these
    same five arguments and supplies a readable body with ``read(size)``
    and ``close()``; ``read1(size)`` is preferred when available. Reads must
    honor the whole-exchange timeout, including opening the response.
    """
    deadline = time.monotonic() + timeout
    opener = build_opener(
        _NoRedirect(),
        ProxyHandler({}),
        _DeadlineHTTPHandler(deadline),
        _DeadlineHTTPSHandler(deadline),
    )
    request = Request(url, data=payload, headers=headers, method=method)
    try:
        response = opener.open(request, timeout=_remaining(deadline))
    except HTTPError as error:
        response = error
    try:
        _remaining(deadline)
        return (
            response.status,
            {key.lower(): value for key, value in response.headers.items()},
            response,
        )
    except BaseException:
        response.close()
        raise


def _http_transport(method, url, headers, payload, timeout):
    """Return status/headers/body; never follow redirects or environment proxies.

    This is the default ``transport`` of TldwClient; a replacement must
    follow the same contract.

    Args:
        method: HTTP method.
        url: Absolute request URL.
        headers: Request headers.
        payload: Request body bytes, or None.
        timeout: Seconds allowed for the whole exchange.

    Returns:
        ``(status, headers, body)`` for any HTTP status, with lowercase header
        names. At most ``_MAX_BODY + 1`` bytes are read, so the caller can
        detect an oversized body.

    Raises:
        TimeoutError, URLError, OSError or HTTPException on transport failure.
    """
    deadline = time.monotonic() + timeout
    status, response_headers, response = _http_stream_transport(
        method, url, headers, payload, _remaining(deadline)
    )
    with response:
        _remaining(deadline)
        body = response.read(_MAX_BODY + 1)
        _remaining(deadline)
        return (
            status,
            response_headers,
            body,
        )


class TldwClient:
    """Synchronous client; ``cancel_event`` cooperatively stops bounded reads.

    An in-flight HTTP read can finish before cancellation is observed; it is
    bounded by request_timeout_seconds (or a stream's explicit timeout_seconds).
    Only an explicit cancel_job call stops server work.

    Args:
        api_url: API origin. Defaults to ``TLDW_API_URL``, then
            ``https://api.tldw.sh``. Must be HTTPS without credentials,
            query, fragment or path; plain HTTP is allowed only for
            localhost, 127.0.0.1 and ::1.
        token: Scoped API key, sent as a Bearer token. Required unless
            ``allow_anonymous`` is set for a loopback origin. The client never
            reads a token from the environment by itself.
        allow_anonymous: Allow requests without a token; only effective on a
            loopback (local development) origin.
        request_timeout_seconds: Budget for one API call, retries included
            (0.001 to 120, default 30). A wait gives each poll the rest of
            its own, longer budget.
        transport: Optional replacement for the HTTP layer, for example in
            tests. See ``_http_transport`` for the contract.
        stream_transport: Optional streaming HTTP layer. See
            ``_http_stream_transport`` for the ownership/timeout contract.

    Raises:
        ValueError: For an invalid origin, token or timeout, or a missing
            token.

    A key needs ``jobs:write`` to submit work and search, ``jobs:read`` to
    poll and list jobs, ``summaries:read`` to read summaries and transcripts,
    and optionally ``account:read`` for ``get_usage``.

    Methods accept ``cancel_event``, a ``threading.Event``. Setting it makes a
    pending wait or retry raise TldwCanceledError promptly.

    Example::

        client = TldwClient(token=os.environ["TLDW_API_TOKEN"])
        result = client.analyze(
            "https://www.youtube.com/watch?v=VIDEO_ID",
            instructions="List the key claims with timestamps.",
            max_credits=30,
        )
    """

    def __init__(
        self,
        *,
        api_url=None,
        token=None,
        allow_anonymous=False,
        request_timeout_seconds=30,
        transport=None,
        stream_transport=None,
        _clock=time.monotonic,
        _sleep=time.sleep,
    ):
        # _clock and _sleep are test hooks for the monotonic clock and sleep.
        if api_url is None:
            api_url = os.environ.get("TLDW_API_URL") or DEFAULT_API_URL
        try:
            parsed = urlsplit(api_url)
            host = parsed.hostname
            port = parsed.port
        except (TypeError, ValueError):
            raise ValueError("api_url must be an absolute HTTPS origin.") from None
        loopback = host in {"localhost", "127.0.0.1", "::1"}
        if (
            not host
            or (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback))
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(
                "api_url must be an HTTPS origin without credentials, query, fragment or path; HTTP is allowed only on loopback."
            )
        if token is not None and (
            not isinstance(token, str)
            or not token
            or not all(33 <= ord(char) <= 126 for char in token)
        ):
            raise ValueError(
                "token must be nonempty printable ASCII without whitespace."
            )
        if not token and not (allow_anonymous and loopback):
            raise ValueError(
                "A tldw API token is required. Anonymous access requires explicit loopback development mode."
            )
        # Normalize the origin while retaining IPv6 brackets and a configured port.
        authority = f"[{host}]" if ":" in host else host
        if port is not None:
            authority += f":{port}"
        self.api_url = f"{parsed.scheme}://{authority}"
        # Name-mangled so the token is not an ordinary public attribute; it is
        # never placed in errors.
        self.__token = token
        self.request_timeout_seconds = _bound(
            request_timeout_seconds, "request_timeout_seconds", 0.001, 120
        )
        self._transport = transport or _http_transport
        self._stream_transport = stream_transport or _http_stream_transport
        self._clock = _clock
        self._sleep = _sleep
        # (kind, id) -> (not_before, interval): polling guidance remembered
        # from submissions, used by the first wait on that resource.
        self._poll_hints = {}

    def _check_canceled(self, cancel_event):
        if cancel_event is not None and cancel_event.is_set():
            raise TldwCanceledError()

    def _pause(self, seconds, cancel_event):
        # Sleep on the event itself so setting it interrupts the pause at once.
        self._check_canceled(cancel_event)
        if cancel_event is None:
            self._sleep(seconds)
        elif cancel_event.wait(seconds):
            raise TldwCanceledError()

    def _response_error(self, data, status, headers):
        """Use the same redaction and error contract for buffered and SSE reads."""
        detail = data.get("error") if isinstance(data, dict) else {}
        detail = detail if isinstance(detail, dict) else {}

        def safe(value, fallback=None):
            value = value if isinstance(value, str) else fallback
            return (
                value.replace(self.__token, "[REDACTED]")
                if value and self.__token
                else value
            )

        extra = detail.get("details")
        extra = (
            _safe_error_resource(extra, self.__token)
            if isinstance(extra, dict)
            else None
        )
        try:
            active_job = _identifier((extra or {}).get("job_id"))
        except ValueError:
            active_job = None
        return TldwError(
            safe(detail.get("message"), f"API request failed ({status})."),
            code=safe(detail.get("code"), "HTTP_ERROR"),
            status=status,
            request_id=safe(detail.get("request_id") or headers.get("x-request-id")),
            retry_after_seconds=_retry_delay(headers.get("retry-after")),
            job_id=active_job,
            details=extra,
        )

    def _request(
        self,
        method,
        path,
        *,
        body=None,
        idempotency_key=None,
        cancel_event=None,
        deadline=None,
    ):
        """Send one API request with bounded retries.

        Retry policy (at most 4 attempts, all before ``deadline``):

        * 429/502/503/504: wait max(Retry-After, backoff) and try again; the
          backoff is 0.5 s, 1 s, then 2 s.
        * Network failure: back off the same way and try again. For a POST
          this resends the identical body and Idempotency-Key, which is safe.
        * Timeout while waiting for the response: a GET is retried; any other
          method is not, because its outcome is unknown.
        * Every other non-2xx status fails at once.

        Args:
            method: HTTP method.
            path: Path and query, built only from validated values.
            body: JSON-serializable request body, or None.
            idempotency_key: Sent as the ``Idempotency-Key`` header.
            cancel_event: Optional ``threading.Event``.
            deadline: Absolute ``_clock()`` time for the whole operation.
                Defaults to now + ``request_timeout_seconds``; waits pass
                their own deadline so a slow poll can be retried within it.

        Returns:
            ``(data, retry_after_seconds)``: the parsed JSON object and the
            parsed ``Retry-After`` header (or None).

        Raises:
            TldwError, TldwTimeoutError or TldwCanceledError.
        """
        deadline = (
            self._clock() + self.request_timeout_seconds
            if deadline is None
            else deadline
        )
        # Serialize once: every retry must send byte-identical content.
        payload = (
            json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
            if body is not None
            else None
        )
        headers = {"Accept": "application/json"}
        if self.__token:
            headers["Authorization"] = f"Bearer {self.__token}"
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        for attempt in range(4):
            self._check_canceled(cancel_event)
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise TldwTimeoutError()
            try:
                status, response_headers, raw = self._transport(
                    method,
                    self.api_url + path,
                    dict(headers),
                    payload,
                    min(remaining, self.request_timeout_seconds),
                )
            except TimeoutError:
                # Raised while waiting for or reading the response. urllib
                # wraps a timeout during connect or send in URLError, which is
                # handled as a network failure below.
                if method != "GET" or self._clock() >= deadline:
                    raise TldwTimeoutError(
                        "The API request timed out. Accepted work continues on the server."
                        if method == "GET"
                        else "The API request timed out. Retry submission with the same idempotency key if its outcome is unknown."
                    ) from None
                # A slow read (for example one poll inside a long wait) has no side
                # effects; retry it while the caller's deadline allows.
                error = TldwTimeoutError(
                    "The API request timed out. Accepted work continues on the server."
                )
                if attempt == 3:
                    raise error from None
                delay = min(10, 0.5 * 2**attempt)
            except (URLError, OSError, HTTPException):
                # Could not connect, send or complete the HTTP exchange.
                # Retried for every method with the same Idempotency-Key.
                if self._clock() >= deadline:
                    raise TldwTimeoutError() from None
                error = TldwError(
                    "Could not reach the configured tldw API. Check the connection before retrying.",
                    code="NETWORK_ERROR",
                )
                if attempt == 3:
                    raise error from None
                delay = min(10, 0.5 * 2**attempt)
            else:
                # A response arrived. It is discarded if the caller canceled
                # or the deadline passed in the meantime.
                self._check_canceled(cancel_event)
                if self._clock() >= deadline:
                    raise TldwTimeoutError()
                if len(raw) > _MAX_BODY:
                    raise TldwError(
                        "API response exceeded the client size limit.",
                        code="INVALID_RESPONSE",
                    )
                retry_after = _retry_delay(response_headers.get("retry-after"))
                try:
                    data = json.loads(raw)
                except (ValueError, UnicodeError, RecursionError):
                    # A proxy's 502/503 page is often HTML: retry it rather
                    # than fail on the parse error.
                    if status not in _TRANSIENT:
                        raise TldwError(
                            "API returned invalid JSON.",
                            code="INVALID_RESPONSE",
                            status=status,
                        ) from None
                    data = {}
                if 200 <= status < 300:
                    return _object(data, "JSON"), retry_after
                error = self._response_error(data, status, response_headers)
                if (
                    status not in _TRANSIENT
                    or error.code in _NON_RETRYABLE_CODES
                    or attempt == 3
                ):
                    raise error
                delay = max(retry_after or 0, min(10, 0.5 * 2**attempt))
            # Never sleep past the deadline. Prefer the last API error (it has
            # a status and code the caller can act on) over a generic timeout.
            if self._clock() + delay >= deadline:
                if error.status:
                    raise error
                raise TldwTimeoutError()
            self._pause(delay, cancel_event)
        # Unreachable: the final attempt always returns or raises above.
        raise TldwError("API retry limit reached.", code="RETRY_LIMIT")

    def search_videos(
        self,
        query,
        *,
        sort="top",
        limit=20,
        platform="x",
        idempotency_key=None,
        cancel_event=None,
    ):
        """Search X posts with video: one page of up to 20 results, no next page.

        A new search uses one of the plan's monthly searches, and the count
        resets each billing period; the same query and sort within 15 minutes
        is replayed without counting. Pass a result's url
        to submit_analysis after checking video.over_plan_limit. Result text is
        untrusted post content. Requires jobs:write.

        Args:
            query: Search text; X operators such as ``from:`` work.
            sort: ``"top"`` (default) or ``"latest"``.
            limit: 1 to 20 (default 20). Trims the page; the search still
                counts once.
            platform: ``"x"``, the only supported platform.
            idempotency_key: Generated when omitted. A retry with the same key
                returns the saved results without counting again.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``search_id``, ``query``, ``sort``, ``replayed``,
            ``results`` (each with ``url``, ``text``, ``author``,
            ``metrics`` and ``video``) and ``usage`` (``searches_used``,
            ``searches_limit``, ``searches_remaining``, ``period_end``).

        Raises:
            ValueError: For invalid arguments.
            TldwError: For example ``SEARCH_QUOTA_EXCEEDED`` (see
                ``details["upgrade"]["billing_url"]``) or ``FEATURE_DISABLED``.
                The error's ``idempotency_key`` replays the same search.

        Example::

            found = client.search_videos("rust async", sort="latest", limit=5)
            for result in found["results"]:
                print(result["url"], result["video"]["duration_seconds"])
        """
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string.")
        if sort not in ("top", "latest"):
            raise ValueError("sort must be top or latest.")
        if platform != "x":
            raise ValueError("platform must be x.")
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 20
        ):
            raise ValueError("limit must be an integer between 1 and 20.")
        # Retries resend this key, so the server replays a finished search
        # instead of counting it again.
        stable_key = _idempotency_key(idempotency_key)
        try:
            data, _ = self._request(
                "POST",
                "/v1/searches",
                body={
                    "query": query,
                    "platform": platform,
                    "sort": sort,
                    "limit": limit,
                },
                idempotency_key=stable_key,
                cancel_event=cancel_event,
            )
        except TldwError as error:
            error.idempotency_key = stable_key
            raise
        results = data.get("results")
        if (
            not isinstance(data.get("search_id"), str)
            or not isinstance(results, list)
            or not all(
                isinstance(item, dict) and isinstance(item.get("url"), str)
                for item in results
            )
            or not isinstance(data.get("usage"), dict)
        ):
            error = TldwError(
                "API returned an invalid search.", code="INVALID_RESPONSE"
            )
            error.idempotency_key = stable_key
            raise error
        return data

    def submit_analysis(
        self,
        media,
        *,
        instructions=None,
        idempotency_key=None,
        cancel_event=None,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
    ):
        """Submit text or a public URL and retain the returned job_id before waiting.

        X URLs default to auto: accessible captions for one attached video are
        included when the deployment supports it. Explicit scope=post reads
        only post text. Use instructions for focus or a requested output language
        (for example, 'Summarize the spoken content in Spanish.'). Omit
        transcription and force_asr: these legacy fields are accepted but
        ignored. Processing uses captions or saved source only; fresh audio
        transcription is unavailable.
        Reuse the exact media, instructions and idempotency_key
        after an unknown submission outcome.
        X post and X Article sources may reuse a saved snapshot for 24 hours. Set
        refresh_source=True to fetch current source content without forcing ASR.

        Returns as soon as the job is accepted (HTTP 202); it does not wait.

        Args:
            media: A YouTube or public X URL string; an explicit X
                source such as ``{"type": "x", "url": url, "scope": "post"}``;
                or ``{"type": "text", "text": text}`` for complete text you
                supply.
            instructions: Analysis focus, format or output language.
            idempotency_key: Generated when omitted. Replaying the same
                request with the same key returns the original job
                (``replayed`` is true) instead of creating another.
            cancel_event: Optional ``threading.Event``.
            transcription: Deprecated compatibility field, accepted but ignored.
                Processing uses captions or saved source only.
            max_credits: Credit ceiling for this job: a positive decimal up to
                1,000,000 with at most two decimal places.
            force_asr: Deprecated compatibility field, accepted but ignored.
            refresh_source: Fetch current X post or Article content instead of
                reusing a snapshot saved within 24 hours.

        Returns:
            dict: The receipt, with ``job_id``, the initial ``job`` and
            polling hints such as ``next_poll_after_ms``.

        Raises:
            ValueError: For invalid arguments (nothing is sent).
            TldwError: With ``idempotency_key`` set, for example
                ``QUOTA_EXCEEDED``, ``VIDEO_TOO_LONG`` or
                ``DUPLICATE_ACTIVE_JOB`` (see ``job_id``).

        Example::

            receipt = client.submit_analysis(
                {"type": "text", "text": thread_text},
                instructions="State the author's argument and the evidence.",
                idempotency_key="thread-request-001",
            )
            save(receipt["job_id"])  # persist before waiting
        """
        return self._submit_media(
            media,
            instructions=instructions,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
        )

    def _submit_media(
        self,
        media,
        *,
        instructions=None,
        idempotency_key=None,
        cancel_event=None,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
    ):
        # Every argument is validated before anything is sent.
        if refresh_source is not None and not isinstance(refresh_source, bool):
            raise ValueError("refresh_source must be a boolean.")
        stable_key = _idempotency_key(idempotency_key)
        body = {
            **_spending_options(transcription, max_credits, force_asr),
        }
        if media is None:
            raise ValueError("Provide media.")
        if not isinstance(media, str) and (
            not isinstance(media, dict)
            or not isinstance(media.get("type", "auto"), str)
            or media.get("type", "auto") not in {"auto", "text", "url", "youtube", "x"}
        ):
            raise ValueError(
                "Provide source text, a YouTube/public X URL, or a media object "
                "with type auto, text, url, youtube or x."
            )
        body["media"] = media
        if instructions is not None:
            body["instructions"] = instructions
        if refresh_source is not None:
            body["refresh_source"] = refresh_source
        try:
            data, retry_after = self._request(
                "POST",
                "/v1/analyses",
                body=body,
                idempotency_key=stable_key,
                cancel_event=cancel_event,
            )
        except TldwError as error:
            error.idempotency_key = stable_key
            raise
        try:
            _identifier(data.get("job_id"), "Accepted job ID")
            if (
                not isinstance(data.get("job"), dict)
                or data["job"].get("id") != data["job_id"]
            ):
                raise ValueError()
        except ValueError:
            error = TldwError(
                "API returned an invalid accepted job.", code="INVALID_RESPONSE"
            )
            error.idempotency_key = stable_key
            try:
                error.job_id = _identifier(data.get("job_id"))
            except ValueError:
                pass
            else:
                error.receipt = _safe_error_resource(data, self.__token)
            raise error from None
        try:
            self._job(data["job"], data["job_id"])
        except TldwError as error:
            error.idempotency_key = stable_key
            error.job_id = data["job_id"]
            error.receipt = _safe_error_resource(data, self.__token)
            raise
        self._remember_poll_hint(
            data["job_id"], "job", data.get("next_poll_after_ms"), retry_after
        )
        return data

    def _remember_poll_hint(self, resource_id, kind, suggested, retry_after):
        """Remember a just-accepted resource's polling guidance.

        ``suggested`` is ``next_poll_after_ms`` and ``retry_after`` the parsed
        Retry-After in seconds. The next wait on the resource will not poll
        earlier or more often than this. At most 256 hints are kept; dicts
        preserve insertion order, so the oldest is evicted first.
        """
        suggested = (
            min(3600, suggested / 1000)
            if isinstance(suggested, (int, float))
            and math.isfinite(suggested)
            and suggested > 0
            else 0
        )
        delay = max(suggested, retry_after or 0)
        if delay:
            if len(self._poll_hints) >= 256:
                self._poll_hints.pop(next(iter(self._poll_hints)))
            self._poll_hints[(kind, resource_id)] = (self._clock() + delay, delay)

    def _job(self, data, resource_id):
        """Validate a job object and return it.

        Checks the expected ID, a known status, an integer progress from 0 to
        100 and a known output mode. A completed job must name its result:
        ``transcript_id`` for transcript-mode jobs, otherwise ``summary_id``.
        """
        _object(data, "job")
        progress = data.get("progress")
        if (
            data.get("id") != resource_id
            or ("job_id" in data and data["job_id"] != resource_id)
            or not isinstance(data.get("status"), str)
            or data.get("status") not in _JOB_STATES
            or not isinstance(progress, int)
            or isinstance(progress, bool)
            or not 0 <= progress <= 100
            or (
                data.get("output_mode") is not None
                and (
                    not isinstance(data["output_mode"], str)
                    or data["output_mode"] not in {"summary", "analysis", "transcript"}
                )
            )
        ):
            raise TldwError(
                "API returned an invalid job.",
                code="INVALID_RESPONSE",
                job_id=resource_id,
            )
        if data["status"] == "completed":
            try:
                _identifier(
                    data.get(
                        "transcript_id"
                        if data.get("output_mode") == "transcript"
                        else "summary_id"
                    )
                )
            except ValueError:
                raise TldwError(
                    "Completed job has no valid result ID.",
                    code="INVALID_RESPONSE",
                    job_id=resource_id,
                ) from None
        return data

    def get_job(self, job_id, *, cancel_event=None):
        """Read a job's current state once, without waiting.

        Args:
            job_id: Job UUID.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: The job: ``id``, ``status``, ``progress`` (0-100) and, once
            completed, ``summary_id`` (or ``transcript_id`` for
            transcript-mode jobs), plus ``error`` and ``credits`` when present.

        Raises:
            ValueError: When ``job_id`` is not a UUID.
            TldwError: For example a 404 for an unknown job.
        """
        job_id = _identifier(job_id)
        data, _ = self._request("GET", f"/v1/jobs/{job_id}", cancel_event=cancel_event)
        return self._job(data, job_id)

    def stream_job_events(
        self, job_id, *, timeout_seconds=None, cancel_event=None
    ) -> Iterator[JobEvent]:
        """Read one owned job's SSE connection, without reconnecting or submitting.

        Yields ``{event, id, data}``, where data passes the same Job validator
        as get_job(). Heartbeat comments are ignored. A terminal snapshot or
        terminal event closes the response before it is yielded. A clean EOF
        ends this iterator; resume explicitly with this method or get_job().

        The single overall deadline defaults to request_timeout_seconds;
        timeout_seconds may be 0.001-610 to cover the server's 600-second
        stream window. Cancellation is checked before and after every read.
        As with other synchronous methods, an in-flight read can continue
        until this deadline. Close the iterator when stopping early (for
        example with contextlib.closing); this never cancels the server job.
        """
        job_id = _identifier(job_id)
        timeout = (
            self.request_timeout_seconds
            if timeout_seconds is None
            else _bound(timeout_seconds, "timeout_seconds", 0.001, 610)
        )

        def iterate():
            deadline = self._clock() + timeout
            body = None
            status = None
            response_headers = {}

            def close_body():
                nonlocal body
                response, body = body, None
                if response is not None:
                    try:
                        response.close()
                    except (OSError, HTTPException):
                        # Cleanup cannot replace the original API/parse error,
                        # and a failed close must not trigger a second request.
                        pass

            def attach(error):
                error.job_id = job_id
                if error.status is None:
                    error.status = status
                if error.request_id is None:
                    request_id = response_headers.get("x-request-id")
                    error.request_id = (
                        request_id.replace(self.__token, "[REDACTED]")
                        if request_id and self.__token
                        else request_id
                    )
                return error

            def check():
                if cancel_event is not None and cancel_event.is_set():
                    raise attach(TldwCanceledError())
                if self._clock() >= deadline:
                    raise attach(TldwTimeoutError())

            def invalid(message):
                return TldwError(message, code="INVALID_RESPONSE")

            def read(size):
                check()
                chunk = getattr(body, "read1", body.read)(size)
                check()
                if not isinstance(chunk, bytes) or len(chunk) > size:
                    raise invalid("API returned an invalid stream body.")
                return chunk

            def lines():
                line = bytearray()
                skip_lf = False
                first = True
                while True:
                    chunk = read(_STREAM_CHUNK_BYTES)
                    if not chunk:
                        if line:
                            raise invalid("API ended an incomplete event line.")
                        return
                    for byte in chunk:
                        if skip_lf:
                            skip_lf = False
                            if byte == 10:
                                continue
                        if byte in (10, 13):
                            try:
                                text = line.decode("utf-8")
                            except UnicodeError:
                                raise invalid(
                                    "API returned invalid event encoding."
                                ) from None
                            if first:
                                text = text.removeprefix("\ufeff")
                                first = False
                            line.clear()
                            skip_lf = byte == 13
                            yield text
                        else:
                            line.append(byte)
                            if len(line) > _MAX_EVENT_BYTES:
                                raise invalid(
                                    "API event line exceeded the client size limit."
                                )

            try:
                check()
                headers = {"Accept": "text/event-stream"}
                if self.__token:
                    headers["Authorization"] = f"Bearer {self.__token}"
                status, response_headers, body = self._stream_transport(
                    "GET",
                    f"{self.api_url}/v1/jobs/{job_id}/events",
                    headers,
                    None,
                    deadline - self._clock(),
                )
                check()
                if not 200 <= status < 300:
                    raw = bytearray()
                    while len(raw) <= _MAX_BODY:
                        chunk = read(min(_STREAM_CHUNK_BYTES, _MAX_BODY + 1 - len(raw)))
                        if not chunk:
                            break
                        raw.extend(chunk)
                    if len(raw) > _MAX_BODY:
                        raise invalid("API response exceeded the client size limit.")
                    try:
                        data = json.loads(raw)
                    except (ValueError, UnicodeError, RecursionError):
                        data = {}
                    raise self._response_error(data, status, response_headers)
                if (
                    response_headers.get("content-type", "")
                    .split(";", 1)[0]
                    .strip()
                    .lower()
                    != "text/event-stream"
                ):
                    raise invalid("API did not return an event stream.")
                kind = ""
                event_id = None
                data_lines = []
                event_bytes = 0
                for line in lines():
                    check()
                    if not line:
                        if data_lines:
                            if kind not in {
                                "snapshot",
                                "status",
                                "completed",
                                "failed",
                                "canceled",
                            }:
                                raise invalid("API returned an unknown job event.")
                            try:
                                data = json.loads("\n".join(data_lines))
                            except (ValueError, UnicodeError, RecursionError):
                                raise invalid(
                                    "API returned invalid event JSON."
                                ) from None
                            data = self._job(data, job_id)
                            terminal = data["status"] in {
                                "completed",
                                "failed",
                                "canceled",
                            }
                            if (kind == "status" and terminal) or (
                                kind not in {"snapshot", "status"}
                                and kind != data["status"]
                            ):
                                raise invalid("API returned an inconsistent job event.")
                            event: JobEvent = {
                                "event": kind,
                                "id": event_id,
                                "data": data,
                            }
                            if terminal:
                                close_body()
                            yield event
                            if terminal:
                                return
                        kind, event_id, data_lines, event_bytes = "", None, [], 0
                        continue
                    if line.startswith(":"):
                        continue
                    event_bytes += len(line.encode("utf-8")) + 1
                    if event_bytes > _MAX_EVENT_BYTES:
                        raise invalid("API event exceeded the client size limit.")
                    field, _, value = line.partition(":")
                    value = value.removeprefix(" ")
                    if field == "event":
                        kind = value
                    elif field == "id":
                        if (
                            not value.isascii()
                            or not value.isdecimal()
                            or not 0 < len(value) <= 64
                            or int(value) <= 0
                        ):
                            raise invalid("API returned an invalid event ID.")
                        event_id = value
                    elif field == "data":
                        data_lines.append(value)
                if data_lines or kind or event_id is not None:
                    raise invalid("API ended an incomplete job event.")
            except TimeoutError:
                raise attach(TldwTimeoutError()) from None
            except (URLError, OSError, HTTPException):
                check()
                raise attach(
                    TldwError(
                        "Could not read the configured tldw API event stream.",
                        code="NETWORK_ERROR",
                    )
                ) from None
            except TldwError as error:
                raise attach(error)
            finally:
                close_body()

        return iterate()

    def wait_job(
        self, job_id, *, wait_seconds=8, include_result=False, cancel_event=None
    ):
        """One bounded server wait; wait_for_job() instead waits until terminal.

        wait_seconds is 0-10. Inline generated output is disabled by default;
        inline output is included only when the credential has summaries:read.
        """
        job_id = _identifier(job_id)
        _bound(wait_seconds, "wait_seconds", 0, 10)
        if type(include_result) is not bool:
            raise ValueError("include_result must be a boolean.")
        query = urlencode(
            {
                "wait_seconds": wait_seconds,
                "include_result": str(include_result).lower(),
            }
        )
        data, retry_after = self._request(
            "GET", f"/v1/jobs/{job_id}/wait?{query}", cancel_event=cancel_event
        )
        if (
            data.get("job_id") != job_id
            or type(data.get("terminal")) is not bool
            or type(data.get("result_available")) is not bool
        ):
            raise TldwError(
                "API returned an invalid job receipt.",
                code="INVALID_RESPONSE",
                job_id=job_id,
            )
        self._job(data.get("job"), job_id)
        if data["terminal"] != (
            data["job"]["status"] in {"completed", "failed", "canceled"}
        ):
            raise TldwError(
                "API returned an inconsistent job receipt.",
                code="INVALID_RESPONSE",
                job_id=job_id,
            )
        self._remember_poll_hint(
            job_id, "job", data.get("next_poll_after_ms"), retry_after
        )
        return data

    def list_jobs(self, *, limit=20, cursor=None, cancel_event=None):
        """List owned jobs without processing or following returned URLs.

        Jobs come newest first, one page at a time.

        Args:
            limit: Page size, 1 to 100 (default 20).
            cursor: ``next_cursor`` from the previous page.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``items`` (jobs) and ``next_cursor`` (None on the last page).

        Raises:
            ValueError: For an invalid limit or cursor.
            TldwError: For API or response errors.

        Example::

            page = client.list_jobs(limit=50)
            while page["next_cursor"]:
                page = client.list_jobs(limit=50, cursor=page["next_cursor"])
        """
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be an integer between 1 and 100.")
        query = {"limit": str(limit)}
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor or len(cursor) > 1000:
                raise ValueError("cursor must be a bounded non-empty string.")
            query["cursor"] = cursor
        data, _ = self._request(
            "GET", "/v1/jobs?" + urlencode(query), cancel_event=cancel_event
        )
        if (
            not isinstance(data.get("items"), list)
            or (
                data.get("next_cursor") is not None
                and (
                    not isinstance(data["next_cursor"], str)
                    or not data["next_cursor"]
                    or len(data["next_cursor"]) > 1000
                )
            )
            or "next_cursor" not in data
        ):
            raise TldwError(
                "API returned an invalid job list.", code="INVALID_RESPONSE"
            )
        for item in data["items"]:
            try:
                identity = (
                    _identifier(item.get("id")) if isinstance(item, dict) else None
                )
            except ValueError:
                identity = None
            if identity is None:
                raise TldwError(
                    "API returned an invalid job list.", code="INVALID_RESPONSE"
                )
            self._job(item, identity)
        return data

    def cancel_job(self, job_id, *, cancel_event=None):
        """Explicitly stop an owned job. Canceling a local wait never invokes this.

        Args:
            job_id: Job UUID.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``id`` and the terminal ``status``, which is ``completed``
            or ``failed`` when the job finished before it could be canceled.

        Raises:
            ValueError: When ``job_id`` is not a UUID.
            TldwError: For API or response errors.
        """
        job_id = _identifier(job_id)
        data, _ = self._request(
            "POST", f"/v1/jobs/{job_id}/cancel", cancel_event=cancel_event
        )
        if (
            data.get("id") != job_id
            or not isinstance(data.get("status"), str)
            or data.get("status") not in {"completed", "failed", "canceled"}
        ):
            raise TldwError(
                "API returned an invalid cancellation result.",
                code="INVALID_RESPONSE",
                job_id=job_id,
            )
        return data

    def get_summary(self, summary_id, *, cancel_event=None):
        """Read a generated summary or analysis. Reading it again is free.

        Args:
            summary_id: Summary UUID, usually ``job["summary_id"]``.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: The summary; its ``content`` dict holds the generated output.

        Raises:
            ValueError: When ``summary_id`` is not a UUID.
            TldwError: For API or response errors.
        """
        summary_id = _identifier(summary_id)
        data, _ = self._request(
            "GET", f"/v1/summaries/{summary_id}", cancel_event=cancel_event
        )
        if data.get("id") != summary_id or not isinstance(data.get("content"), dict):
            raise TldwError("API returned an invalid summary.", code="INVALID_RESPONSE")
        return data

    def get_usage(self, *, cancel_event=None):
        """Read the account's plan, credit usage and limits (GET /v1/me).

        Requires ``account:read``; the response is returned as-is.

        Raises:
            TldwError: A 403 when the key lacks ``account:read``.
        """
        return self._request("GET", "/v1/me", cancel_event=cancel_event)[0]

    def get_transcript(self, summary_id, *, cancel_event=None):
        """Read the owner-private saved source separately from generated output.

        Timed video sources return segments; text/X sources return blocks and
        text. X caption blocks retain start_s/end_s. Requires summaries:read.

        Starts no processing.

        Args:
            summary_id: Summary UUID, usually ``job["summary_id"]``.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``transcript_id``, ``timed``, ``source``, ``language``,
            ``segments``, ``blocks`` and ``text`` (None for timed sources).

        Raises:
            ValueError: When ``summary_id`` is not a UUID.
            TldwError: For API or response errors.
        """
        summary_id = _identifier(summary_id)
        data, _ = self._request(
            "GET", f"/v1/summaries/{summary_id}/transcript", cancel_event=cancel_event
        )
        return self._transcript(data)

    def get_transcript_page(
        self, summary_id, *, offset=0, limit=100, cancel_event=None
    ):
        """Read one bounded original-source page behind a summary; starts no work."""
        summary_id = _identifier(summary_id)
        _page_options(offset, limit)
        data, _ = self._request(
            "GET",
            f"/v1/summaries/{summary_id}/transcript/page?offset={offset}&limit={limit}",
            cancel_event=cancel_event,
        )
        return self._transcript_page(data, offset, limit)

    def _transcript(self, data, expected_id=None):
        """Validate a transcript body and return it.

        Timed (video) sources carry segments and a None ``text``; untimed
        (text and X) sources carry ``text`` and blocks.
        """
        _object(data, "transcript")
        try:
            _identifier(data.get("transcript_id"))
        except (ValueError, TypeError):
            raise TldwError(
                "API returned an invalid transcript.", code="INVALID_RESPONSE"
            ) from None
        if (
            expected_id is not None
            and data.get("transcript_id") != expected_id
            or not isinstance(data.get("timed"), bool)
            or "text" not in data
            or "language" not in data
            or not isinstance(data.get("source"), str)
            or (
                data.get("language") is not None
                and not isinstance(data["language"], str)
            )
            or not isinstance(data.get("segments"), list)
            or not isinstance(data.get("blocks"), list)
            or (
                data.get("text") is not None
                if data["timed"]
                else not isinstance(data.get("text"), str)
            )
        ):
            raise TldwError(
                "API returned an invalid transcript.", code="INVALID_RESPONSE"
            )
        return data

    def _transcript_page(self, data, offset, limit, expected_id=None):
        self._transcript(data, expected_id)
        count = len(data["segments"] if data["timed"] else data["blocks"])
        total = data.get("total_segments" if data["timed"] else "total_blocks")
        next_offset = data.get("next_offset")
        if (
            type(data.get("total_segments")) is not int
            or data["total_segments"] < 0
            or type(data.get("total_blocks")) is not int
            or data["total_blocks"] < 0
            or count > limit
            or count > max(0, total - offset)
            or (
                data["total_blocks"] != 0
                if data["timed"]
                else data["total_segments"] != 0
            )
            or (bool(data["blocks"]) if data["timed"] else bool(data["segments"]))
            or "next_offset" not in data
            or (
                offset + count < total
                if next_offset is None
                else type(next_offset) is not int
                or next_offset != offset + count
                or next_offset <= offset
                or next_offset >= total
            )
        ):
            raise TldwError(
                "API returned an invalid transcript page.", code="INVALID_RESPONSE"
            )
        return data

    def list_transcripts(
        self,
        *,
        limit=50,
        cursor=None,
        since=None,
        until=None,
        unread_only=False,
        cancel_event=None,
    ):
        """List saved-source metadata by latest owned request; never marks read.

        since is inclusive, until exclusive; follow next_cursor with the same filters.
        Timestamp strings use ISO 8601. Requires summaries:read.

        Args:
            limit: Page size, 1 to 100 (default 50).
            cursor: ``next_cursor`` from the previous page.
            since: Inclusive lower bound on the latest request time.
            until: Exclusive upper bound on the latest request time.
            unread_only: Only transcripts not yet marked read.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``items`` (metadata, most recently requested first) and
            ``next_cursor``.

        Raises:
            ValueError: For an invalid limit, cursor or timestamp string.
            TypeError: When ``unread_only`` is not a bool.
            TldwError: For API or response errors.
        """
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be an integer between 1 and 100.")
        if not isinstance(unread_only, bool):
            raise TypeError("unread_only must be a boolean.")
        query = {"limit": str(limit), "unread_only": str(unread_only).lower()}
        for name, value in (("cursor", cursor), ("since", since), ("until", until)):
            if value is None:
                continue
            if (
                not isinstance(value, str)
                or not value
                or len(value) > (1024 if name == "cursor" else 128)
            ):
                raise ValueError(f"{name} must be a bounded non-empty string.")
            query[name] = value
        data, _ = self._request(
            "GET", "/v1/transcripts?" + urlencode(query), cancel_event=cancel_event
        )
        if not isinstance(data.get("items"), list) or (
            data.get("next_cursor") is not None
            and not isinstance(data["next_cursor"], str)
        ):
            raise TldwError(
                "API returned an invalid transcript list.", code="INVALID_RESPONSE"
            )
        return data

    def get_saved_transcript(self, transcript_id, *, cancel_event=None):
        """Read the full original source by transcript id; never marks read.

        Args:
            transcript_id: Transcript UUID, from ``list_transcripts`` or a
                job's ``transcript_id``.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: The transcript, shaped like ``get_transcript``'s result.

        Raises:
            ValueError: When ``transcript_id`` is not a UUID.
            TldwError: For API or response errors.
        """
        transcript_id = _identifier(transcript_id)
        data, _ = self._request(
            "GET", f"/v1/transcripts/{transcript_id}", cancel_event=cancel_event
        )
        return self._transcript(data, transcript_id)

    def get_saved_transcript_metadata(self, transcript_id, *, cancel_event=None):
        """Read source metadata by id without loading text, processing or marking read.

        Args:
            transcript_id: Transcript UUID.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: Metadata such as ``video``, ``source``, ``segment_count``,
            ``unread`` and ``read_at``.

        Raises:
            ValueError: When ``transcript_id`` is not a UUID.
            TldwError: For API or response errors.
        """
        transcript_id = _identifier(transcript_id)
        data, _ = self._request(
            "GET",
            f"/v1/transcripts/{transcript_id}/metadata",
            cancel_event=cancel_event,
        )
        if (
            data.get("id") != transcript_id
            or not isinstance(data.get("unread"), bool)
            or not isinstance(data.get("video"), dict)
            or not isinstance(data["video"].get("id"), str)
        ):
            raise TldwError(
                "API returned invalid transcript metadata.", code="INVALID_RESPONSE"
            )
        return data

    def get_saved_transcript_page(
        self, transcript_id, *, offset=0, limit=100, cancel_event=None
    ):
        """Read a bounded source page; follow next_offset. Never marks read.

        Args:
            transcript_id: Transcript UUID.
            offset: First segment or block to return (default 0).
            limit: Items per page, 1 to 500 (default 100).
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: A transcript page with ``total_segments``, ``total_blocks``
            and ``next_offset`` (None after the last page).

        Raises:
            ValueError: For an invalid ID, offset or limit.
            TldwError: For API or response errors.

        Example::

            offset = 0
            while offset is not None:
                page = client.get_saved_transcript_page(transcript_id, offset=offset)
                handle(page["segments"], page["blocks"])
                offset = page["next_offset"]
        """
        transcript_id = _identifier(transcript_id)
        _page_options(offset, limit)
        data, _ = self._request(
            "GET",
            f"/v1/transcripts/{transcript_id}/page?offset={offset}&limit={limit}",
            cancel_event=cancel_event,
        )
        return self._transcript_page(data, offset, limit, transcript_id)

    def mark_transcript_read(self, transcript_id, read=True, *, cancel_event=None):
        """Explicitly mark your source read/unread; repeated marks are idempotent.

        Args:
            transcript_id: Transcript UUID.
            read: True (default) marks it read, False unread.
            cancel_event: Optional ``threading.Event``.

        Returns:
            dict: ``id``, ``unread`` and ``read_at``.

        Raises:
            ValueError: When ``transcript_id`` is not a UUID.
            TypeError: When ``read`` is not a bool.
            TldwError: For API or response errors.
        """
        transcript_id = _identifier(transcript_id)
        if not isinstance(read, bool):
            raise TypeError("read must be a boolean.")
        data, _ = self._request(
            "PATCH",
            f"/v1/transcripts/{transcript_id}/read",
            body={"read": read},
            cancel_event=cancel_event,
        )
        if (
            data.get("id") != transcript_id
            or not isinstance(data.get("unread"), bool)
            or (
                data.get("read_at") is not None and not isinstance(data["read_at"], str)
            )
        ):
            raise TldwError(
                "API returned an invalid transcript read state.",
                code="INVALID_RESPONSE",
            )
        return data

    def _wait(
        self,
        resource_id,
        kind,
        *,
        timeout_seconds=180,
        poll_interval_seconds=1.5,
        cancel_event=None,
        on_progress=None,
    ):
        """Poll a resource until it reaches a terminal state (see wait_for_job)."""
        deadline = self._clock() + _bound(
            timeout_seconds, "timeout_seconds", 0.001, 3600
        )
        interval = _bound(poll_interval_seconds, "poll_interval_seconds", 0.25, 30)
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable.")
        try:
            # Honor the guidance from the submission before the first poll,
            # and never poll faster than the interval the server asked for.
            remembered = self._poll_hints.get((kind, resource_id))
            if remembered:
                interval = max(interval, remembered[1])
                delay = max(0, remembered[0] - self._clock())
                if self._clock() + delay >= deadline:
                    raise TldwTimeoutError()
                if delay:
                    self._pause(delay, cancel_event)
            while True:
                if self._clock() >= deadline:
                    raise TldwTimeoutError()
                # Each poll may use the rest of the wait's budget, so a slow
                # GET is retried inside the wait.
                data, retry_after = self._request(
                    "GET",
                    f"/v1/jobs/{resource_id}",
                    cancel_event=cancel_event,
                    deadline=deadline,
                )
                resource = self._job(data, resource_id)
                if on_progress:
                    on_progress(resource)
                if resource["status"] == "completed":
                    self._poll_hints.pop((kind, resource_id), None)
                    return resource
                if resource["status"] in {"failed", "canceled"}:
                    self._poll_hints.pop((kind, resource_id), None)
                    raise TldwJobError(
                        _safe_error_resource(resource, self.__token), kind
                    )
                suggested = resource.get("next_poll_after_ms")
                suggested = (
                    min(3600, suggested / 1000)
                    if isinstance(suggested, (int, float))
                    and math.isfinite(suggested)
                    and suggested > 0
                    else 0
                )
                delay = max(interval, retry_after or 0, suggested)
                # Fail now rather than sleep past the deadline.
                if self._clock() + delay >= deadline:
                    raise TldwTimeoutError()
                self._pause(delay, cancel_event)
        except TldwError as error:
            # Tag the failure with the job ID so the caller can resume.
            setattr(error, f"{kind}_id", resource_id)
            raise

    def wait_for_job(self, job_id, **options):
        """Poll a job until it completes, fails or is canceled.

        Polls are spaced at least ``poll_interval_seconds`` apart; the
        server's ``next_poll_after_ms`` and ``Retry-After`` can only lengthen
        the spacing. A timeout or cancellation ends the local wait only: the
        job keeps running and can be resumed with another call.

        Args:
            job_id: Job UUID.
            **options: ``timeout_seconds`` (total wait, 0.001 to 3600,
                default 180), ``poll_interval_seconds`` (0.25 to 30, default
                1.5), ``cancel_event`` (a ``threading.Event``) and
                ``on_progress`` (called with the job after every poll,
                terminal ones included; an exception from it ends the wait).
                Any other keyword raises TypeError.

        Returns:
            dict: The completed job.

        Raises:
            TldwJobError: The job failed or was canceled.
            TldwTimeoutError: The wait budget ran out first.
            TldwCanceledError: ``cancel_event`` was set.
            ValueError: For an invalid ID or option.
            TldwError: For API or response errors. TldwError instances raised
                while waiting carry ``job_id``.

        Example::

            job = client.wait_for_job(
                job_id,
                timeout_seconds=600,
                on_progress=lambda job: print(job["status"], job["progress"]),
            )
        """
        return self._wait(_identifier(job_id), "job", **options)

    def analyze(
        self,
        media,
        *,
        instructions=None,
        idempotency_key=None,
        cancel_event=None,
        timeout_seconds=180,
        on_progress=None,
        poll_interval_seconds=1.5,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
    ):
        """Submit media, wait for the job and read its summary in one call.

        Wait options are validated before submitting, so a bad option never
        creates a job. If a TldwError is raised after the job was accepted,
        it carries ``job_id`` and a redacted ``receipt``: resume with
        ``wait_for_job(error.job_id)`` instead of submitting again.

        Args:
            media: See ``submit_analysis``.
            instructions, idempotency_key, transcription, max_credits,
                force_asr, refresh_source: See ``submit_analysis``.
            cancel_event: Optional ``threading.Event``; stops the wait, not
                the job.
            timeout_seconds, poll_interval_seconds, on_progress: See
                ``wait_for_job``.

        Returns:
            dict: ``{"receipt": ..., "job": ..., "summary": ...}``.

        Raises:
            ValueError: For invalid arguments (nothing is sent).
            TldwError: Including TldwJobError, TldwTimeoutError and
                TldwCanceledError.

        Example::

            try:
                result = client.analyze(url, max_credits=30, timeout_seconds=600)
            except TldwTimeoutError as error:
                job = client.wait_for_job(error.job_id, timeout_seconds=600)
        """
        _bound(timeout_seconds, "timeout_seconds", 0.001, 3600)
        _bound(poll_interval_seconds, "poll_interval_seconds", 0.25, 30)
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable.")
        receipt = self.submit_analysis(
            media,
            instructions=instructions,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
        )
        try:
            job = self.wait_for_job(
                receipt["job_id"],
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                on_progress=on_progress,
                poll_interval_seconds=poll_interval_seconds,
            )
            if job.get("output_mode") == "transcript":
                raise TldwError(
                    "Expected an analysis result, received a transcript job.",
                    code="INVALID_RESPONSE",
                )
            summary = self.get_summary(job["summary_id"], cancel_event=cancel_event)
            return {"receipt": receipt, "job": job, "summary": summary}
        except TldwError as error:
            error.job_id = receipt["job_id"]
            error.receipt = _safe_error_resource(receipt, self.__token)
            raise

    def extract_transcript(
        self,
        media,
        *,
        instructions=None,
        preferred_caption_languages=None,
        idempotency_key=None,
        cancel_event=None,
        timeout_seconds=180,
        on_progress=None,
        poll_interval_seconds=1.5,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
        offset=None,
        limit=None,
    ):
        """Analyze once, wait, then read the saved original transcript separately.

        Normal processing applies: the analysis saves both generated output and its source.

        The summary can be read later with ``get_summary(job["summary_id"])``.
        Arguments match ``analyze``; ``preferred_caption_languages`` is not
        supported and raises ValueError before anything is sent. Failure
        handling also matches ``analyze``.

        Omit ``offset`` and ``limit`` for the full source, or supply either
        to select one bounded page (defaults 0/100, limit at most 500).

        Returns:
            dict: ``{"receipt": ..., "job": ..., "transcript": ...}``.
        """
        if preferred_caption_languages is not None:
            raise ValueError(
                "preferred_caption_languages is unavailable for analysis submissions."
            )
        _bound(timeout_seconds, "timeout_seconds", 0.001, 3600)
        _bound(poll_interval_seconds, "poll_interval_seconds", 0.25, 30)
        paged = offset is not None or limit is not None
        if paged:
            offset, limit = (
                0 if offset is None else offset,
                100 if limit is None else limit,
            )
            _page_options(offset, limit)
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable.")
        receipt = self.submit_analysis(
            media,
            instructions=instructions,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
        )
        try:
            job = self.wait_for_job(
                receipt["job_id"],
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                on_progress=on_progress,
                poll_interval_seconds=poll_interval_seconds,
            )
            if job.get("output_mode") == "transcript":
                raise TldwError(
                    "Expected an analysis result, received a transcript job.",
                    code="INVALID_RESPONSE",
                )
            transcript = (
                self.get_transcript_page(
                    job["summary_id"],
                    offset=offset,
                    limit=limit,
                    cancel_event=cancel_event,
                )
                if paged
                else self.get_transcript(job["summary_id"], cancel_event=cancel_event)
            )
            return {"receipt": receipt, "job": job, "transcript": transcript}
        except TldwError as error:
            error.job_id = receipt["job_id"]
            error.receipt = _safe_error_resource(receipt, self.__token)
            raise
