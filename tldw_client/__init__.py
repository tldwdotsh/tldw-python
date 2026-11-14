"""A dependency-free, scoped-key client for the tldw.sh async API.

No server-returned URLs are followed. Waiting never cancels accepted server work.
"""

from __future__ import annotations

import io
import json
import math
import re
import socket
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from http.client import HTTPConnection, HTTPException, HTTPSConnection
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
    "TldwCanceledError",
    "TldwClient",
    "TldwError",
    "TldwJobError",
    "TldwTimeoutError",
]
__version__ = "0.2.7"
_MAX_BODY = 8 * 1024 * 1024
_TRANSIENT = {429, 502, 503, 504}


def _spending_options(transcription=None, max_credits=None, force_asr=None):
    if transcription is not None and transcription not in ("never", "if_needed"):
        raise ValueError("transcription must be never or if_needed.")
    if force_asr is not None and not isinstance(force_asr, bool):
        raise ValueError("force_asr must be a boolean.")
    if force_asr and transcription != "if_needed":
        raise ValueError("force_asr requires explicit transcription=if_needed.")
    result = {}
    if transcription is not None:
        result["transcription"] = transcription
    if force_asr is not None:
        result["force_asr"] = force_asr
    if max_credits is not None:
        try:
            if isinstance(max_credits, bool):
                raise TypeError
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


class TldwError(Exception):
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
    def __init__(
        self,
        message="The wait ended. Accepted work continues on the server.",
        **details,
    ):
        super().__init__(message, code="WAIT_TIMEOUT", **details)


class TldwCanceledError(TldwError):
    def __init__(self):
        super().__init__(
            "The wait was canceled. Accepted work continues on the server.",
            code="WAIT_CANCELED",
        )


class TldwJobError(TldwError):
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
    try:
        if not isinstance(value, str) or len(value) != 36:
            raise ValueError()
        parsed = uuid.UUID(value)
        if str(parsed) != value.lower():
            raise ValueError()
        return str(parsed)
    except (ValueError, AttributeError):
        raise ValueError(f"{label} must be a UUID.") from None


def _bound(value, label, minimum, maximum):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{label} must be between {minimum} and {maximum}.")
    return float(value)


def _object(value, label):
    if not isinstance(value, dict):
        raise TldwError(f"Invalid {label} response.", code="INVALID_RESPONSE")
    return value


def _idempotency_key(value):
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


def _retry_delay(value):
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
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError()
    return remaining


class _DeadlineReader(io.RawIOBase):
    """Refresh the socket budget before every read, including HTTP framing."""

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
    """Adapt http.client's socket/file interface without background threads."""

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
    def __init__(self, *args, deadline, **kwargs):
        self._deadline = deadline
        super().__init__(*args, **kwargs)
        self._create_connection = self._connect_socket

    def _connect_socket(self, address, timeout, source_address):
        # The system resolver is synchronous and has no portable cancellation
        # API. Check elapsed time immediately afterwards, before opening a socket.
        addresses = socket.getaddrinfo(*address, type=socket.SOCK_STREAM)
        _remaining(self._deadline)
        last_error = None
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


def _http_transport(method, url, headers, payload, timeout):
    """Return status/headers/body; never follow redirects or environment proxies."""
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
    with response:
        _remaining(deadline)
        body = response.read(_MAX_BODY + 1)
        _remaining(deadline)
        return (
            response.status,
            {key.lower(): value for key, value in response.headers.items()},
            body,
        )


class TldwClient:
    """Synchronous client; ``cancel_event`` cooperatively stops bounded polling.

    An in-flight HTTP read can finish before cancellation is observed; it is
    bounded by request_timeout_seconds. The SDK never sends a job cancellation.
    """

    def __init__(
        self,
        *,
        api_url,
        token=None,
        allow_anonymous=False,
        request_timeout_seconds=30,
        transport=None,
        _clock=time.monotonic,
        _sleep=time.sleep,
    ):
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
        self.__token = token
        self.request_timeout_seconds = _bound(
            request_timeout_seconds, "request_timeout_seconds", 0.001, 120
        )
        self._transport = transport or _http_transport
        self._clock = _clock
        self._sleep = _sleep
        self._poll_hints = {}

    def _check_canceled(self, cancel_event):
        if cancel_event is not None and cancel_event.is_set():
            raise TldwCanceledError()

    def _pause(self, seconds, cancel_event):
        self._check_canceled(cancel_event)
        if cancel_event is None:
            self._sleep(seconds)
        elif cancel_event.wait(seconds):
            raise TldwCanceledError()

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
        deadline = (
            self._clock() + self.request_timeout_seconds
            if deadline is None
            else deadline
        )
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
                except (ValueError, UnicodeError):
                    if status not in _TRANSIENT:
                        raise TldwError(
                            "API returned invalid JSON.",
                            code="INVALID_RESPONSE",
                            status=status,
                        ) from None
                    data = {}
                if 200 <= status < 300:
                    return _object(data, "JSON"), retry_after
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
                error = TldwError(
                    safe(detail.get("message"), f"API request failed ({status})."),
                    code=safe(detail.get("code"), "HTTP_ERROR"),
                    status=status,
                    request_id=safe(
                        detail.get("request_id") or response_headers.get("x-request-id")
                    ),
                    retry_after_seconds=retry_after,
                    job_id=active_job,
                    details=extra,
                )
                if status not in _TRANSIENT or attempt == 3:
                    raise error
                delay = max(retry_after or 0, min(10, 0.5 * 2**attempt))
            if self._clock() + delay >= deadline:
                if error.status:
                    raise error
                raise TldwTimeoutError()
            self._pause(delay, cancel_event)
        raise TldwError("API retry limit reached.", code="RETRY_LIMIT")

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
        (for example, 'Summarize the spoken content in Spanish.'). X audio
        transcription and visual analysis are not enabled. Missing captions fail
        explicitly. Reuse the exact media, instructions and idempotency_key
        after an unknown submission outcome.
        X/article sources may reuse a saved snapshot for 24 hours. Set
        refresh_source=True to fetch current source content without forcing ASR.
        """
        return self._submit_media(
            "/v1/analyses",
            media,
            instructions=instructions,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
        )

    def submit_transcript(
        self,
        media=None,
        *,
        upload_id=None,
        preferred_caption_languages=None,
        idempotency_key=None,
        cancel_event=None,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
    ):
        """Save original captions/source text without summary generation or an LLM call."""
        return self._submit_media(
            "/v1/transcripts",
            media,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
            upload_id=upload_id,
            preferred_caption_languages=preferred_caption_languages,
        )

    def _submit_media(
        self,
        path,
        media,
        *,
        instructions=None,
        idempotency_key=None,
        cancel_event=None,
        transcription=None,
        max_credits=None,
        force_asr=None,
        refresh_source=None,
        upload_id=None,
        preferred_caption_languages=None,
    ):
        if refresh_source is not None and not isinstance(refresh_source, bool):
            raise ValueError("refresh_source must be a boolean.")
        stable_key = _idempotency_key(idempotency_key)
        body = {
            **_spending_options(transcription, max_credits, force_asr),
        }
        if path == "/v1/transcripts":
            if (media is not None) == (upload_id is not None):
                raise ValueError("Provide exactly one media or upload_id.")
            if upload_id is not None:
                body["upload_id"] = _identifier(upload_id, "Upload ID")
            if preferred_caption_languages is not None:
                if (
                    not isinstance(preferred_caption_languages, list)
                    or len(preferred_caption_languages) > 10
                    or any(
                        not isinstance(value, str)
                        or not re.fullmatch(
                            r"[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*", value
                        )
                        for value in preferred_caption_languages
                    )
                ):
                    raise ValueError(
                        "preferred_caption_languages must contain at most 10 BCP-47 language tags."
                    )
                body["preferred_caption_languages"] = preferred_caption_languages
        if media is not None:
            body["media"] = media
        if instructions is not None:
            body["instructions"] = instructions
        if refresh_source is not None:
            body["refresh_source"] = refresh_source
        try:
            data, retry_after = self._request(
                "POST",
                path,
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
            if path == "/v1/transcripts" and (
                data.get("output_mode") != "transcript"
                or data["job"].get("output_mode") != "transcript"
            ):
                raise ValueError()
        except ValueError:
            error = TldwError(
                "API returned an invalid accepted job.", code="INVALID_RESPONSE"
            )
            error.idempotency_key = stable_key
            raise error from None
        self._remember_poll_hint(
            data["job_id"], "job", data.get("next_poll_after_ms"), retry_after
        )
        return data

    def _remember_poll_hint(self, resource_id, kind, suggested, retry_after):
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
        _object(data, "job")
        progress = data.get("progress")
        if (
            data.get("id") != resource_id
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
        job_id = _identifier(job_id)
        data, _ = self._request("GET", f"/v1/jobs/{job_id}", cancel_event=cancel_event)
        return self._job(data, job_id)

    def get_summary(self, summary_id, *, cancel_event=None):
        summary_id = _identifier(summary_id)
        data, _ = self._request(
            "GET", f"/v1/summaries/{summary_id}", cancel_event=cancel_event
        )
        if data.get("id") != summary_id or not isinstance(data.get("content"), dict):
            raise TldwError("API returned an invalid summary.", code="INVALID_RESPONSE")
        return data

    def get_usage(self, *, cancel_event=None):
        return self._request("GET", "/v1/me", cancel_event=cancel_event)[0]

    def get_transcript(self, summary_id, *, cancel_event=None):
        """Read the owner-private saved source separately from generated output.

        Timed video sources return segments; text/X sources return blocks and
        text. X caption blocks retain start_s/end_s. Requires summaries:read.
        """
        summary_id = _identifier(summary_id)
        data, _ = self._request(
            "GET", f"/v1/summaries/{summary_id}/transcript", cancel_event=cancel_event
        )
        return self._transcript(data)

    def _transcript(self, data):
        try:
            _identifier(data.get("transcript_id"))
        except (ValueError, TypeError):
            raise TldwError(
                "API returned an invalid transcript.", code="INVALID_RESPONSE"
            ) from None
        if (
            not isinstance(data.get("timed"), bool)
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
        """Read the full original source by transcript id; never marks read."""
        transcript_id = _identifier(transcript_id)
        data, _ = self._request(
            "GET", f"/v1/transcripts/{transcript_id}", cancel_event=cancel_event
        )
        return self._transcript(data)

    def get_saved_transcript_metadata(self, transcript_id, *, cancel_event=None):
        """Read source metadata by id without loading text, processing or marking read."""
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
        """Read a bounded source page; follow next_offset. Never marks read."""
        transcript_id = _identifier(transcript_id)
        for name, value, minimum, maximum in (
            ("offset", offset, 0, 1_000_000_000),
            ("limit", limit, 1, 500),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not minimum <= value <= maximum
            ):
                raise ValueError(
                    f"{name} must be an integer between {minimum} and {maximum}."
                )
        data, _ = self._request(
            "GET",
            f"/v1/transcripts/{transcript_id}/page?offset={offset}&limit={limit}",
            cancel_event=cancel_event,
        )
        return self._transcript(data)

    def mark_transcript_read(self, transcript_id, read=True, *, cancel_event=None):
        """Explicitly mark your source read/unread; repeated marks are idempotent."""
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
        deadline = self._clock() + _bound(
            timeout_seconds, "timeout_seconds", 0.001, 3600
        )
        interval = _bound(poll_interval_seconds, "poll_interval_seconds", 0.25, 30)
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable.")
        try:
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
                if self._clock() + delay >= deadline:
                    raise TldwTimeoutError()
                self._pause(delay, cancel_event)
        except TldwError as error:
            setattr(error, f"{kind}_id", resource_id)
            raise

    def wait_for_job(self, job_id, **options):
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
        media=None,
        *,
        upload_id=None,
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
    ):
        """Submit once, wait, then read original source; never requests generated analysis."""
        _bound(timeout_seconds, "timeout_seconds", 0.001, 3600)
        _bound(poll_interval_seconds, "poll_interval_seconds", 0.25, 30)
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable.")
        receipt = self.submit_transcript(
            media,
            idempotency_key=idempotency_key,
            cancel_event=cancel_event,
            transcription=transcription,
            max_credits=max_credits,
            force_asr=force_asr,
            refresh_source=refresh_source,
            upload_id=upload_id,
            preferred_caption_languages=preferred_caption_languages,
        )
        try:
            job = self.wait_for_job(
                receipt["job_id"],
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                on_progress=on_progress,
                poll_interval_seconds=poll_interval_seconds,
            )
            if job.get("output_mode") != "transcript":
                raise TldwError(
                    "Expected a transcript extraction result.", code="INVALID_RESPONSE"
                )
            transcript = self.get_saved_transcript(
                job["transcript_id"], cancel_event=cancel_event
            )
            return {"receipt": receipt, "job": job, "transcript": transcript}
        except TldwError as error:
            error.job_id = receipt["job_id"]
            error.receipt = _safe_error_resource(receipt, self.__token)
            raise
