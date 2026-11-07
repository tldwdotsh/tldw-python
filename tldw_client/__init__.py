"""A dependency-free, scoped-key client for the tldw.sh async API.

No server-returned URLs are followed. Waiting never cancels accepted server work.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

__all__ = [
    "TldwCanceledError",
    "TldwClient",
    "TldwError",
    "TldwJobError",
    "TldwTimeoutError",
]
__version__ = "0.2.1"
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
    "fetching_metadata",
    "fetching_transcript",
    "downloading_audio",
    "transcribing",
    "summarizing",
    "completed",
    "failed",
    "canceled",
}
_COMPARISON_STATES = {
    "queued",
    "waiting",
    "processing",
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
    ):
        super().__init__(message)
        self.code = code
        self.status = status
        self.request_id = request_id
        self.retry_after_seconds = retry_after_seconds
        self.job_id = job_id
        self.comparison_id = comparison_id


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


def _http_transport(method, url, headers, payload, timeout):
    """Return status/headers/body; never follow redirects or environment proxies."""
    opener = build_opener(_NoRedirect(), ProxyHandler({}))
    request = Request(url, data=payload, headers=headers, method=method)
    try:
        response = opener.open(request, timeout=timeout)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(_MAX_BODY + 1)
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
                raise TldwTimeoutError(
                    "The API request timed out. Retry submission with the same idempotency key if its outcome is unknown."
                ) from None
            except (URLError, OSError):
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

                error = TldwError(
                    safe(detail.get("message"), f"API request failed ({status})."),
                    code=safe(detail.get("code"), "HTTP_ERROR"),
                    status=status,
                    request_id=safe(
                        detail.get("request_id") or response_headers.get("x-request-id")
                    ),
                    retry_after_seconds=retry_after,
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
    ):
        stable_key = _idempotency_key(idempotency_key)
        body = {
            "media": media,
            **_spending_options(transcription, max_credits, force_asr),
        }
        if instructions is not None:
            body["instructions"] = instructions
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
        ):
            raise TldwError(
                "API returned an invalid job.",
                code="INVALID_RESPONSE",
                job_id=resource_id,
            )
        if data["status"] == "completed":
            try:
                _identifier(data.get("summary_id"))
            except ValueError:
                raise TldwError(
                    "Completed job has no valid summary ID.",
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

    def _comparison(self, data, resource_id):
        resource = _object(data.get("comparison"), "comparison")
        if (
            resource.get("id") != resource_id
            or resource.get("status") not in _COMPARISON_STATES
        ):
            raise TldwError(
                "API returned an invalid comparison.",
                code="INVALID_RESPONSE",
                comparison_id=resource_id,
            )
        return resource

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
                path = f"/v1/{'jobs' if kind == 'job' else 'comparisons'}/{resource_id}"
                data, retry_after = self._request(
                    "GET", path, cancel_event=cancel_event, deadline=deadline
                )
                resource = (
                    self._job(data, resource_id)
                    if kind == "job"
                    else self._comparison(data, resource_id)
                )
                if on_progress:
                    on_progress(resource)
                if resource["status"] == "completed":
                    self._poll_hints.pop((kind, resource_id), None)
                    if kind == "comparison" and not resource.get("result"):
                        raise TldwError(
                            "Comparison result access is unavailable; summaries:read is required.",
                            code="RESULT_SCOPE_REQUIRED",
                            comparison_id=resource_id,
                        )
                    if kind == "comparison":
                        _object(resource["result"], "comparison result")
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
        )
        try:
            job = self.wait_for_job(
                receipt["job_id"],
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                on_progress=on_progress,
                poll_interval_seconds=poll_interval_seconds,
            )
            summary = self.get_summary(job["summary_id"], cancel_event=cancel_event)
            return {"receipt": receipt, "job": job, "summary": summary}
        except TldwError as error:
            error.job_id = receipt["job_id"]
            error.receipt = _safe_error_resource(receipt, self.__token)
            raise

    def compare_tiktoks(
        self,
        urls,
        *,
        instructions=None,
        idempotency_key=None,
        cancel_event=None,
        transcription=None,
        max_credits=None,
        force_asr=None,
    ):
        if not isinstance(urls, list) or not 2 <= len(urls) <= 20:
            raise ValueError("Supply between 2 and 20 TikTok URLs.")
        stable_key = _idempotency_key(idempotency_key)
        body = {
            "urls": urls,
            **_spending_options(transcription, max_credits, force_asr),
        }
        if instructions is not None:
            body["instructions"] = instructions
        try:
            data, retry_after = self._request(
                "POST",
                "/v1/comparisons",
                body=body,
                idempotency_key=stable_key,
                cancel_event=cancel_event,
            )
        except TldwError as error:
            error.idempotency_key = stable_key
            raise
        try:
            resource_id = _identifier(
                _object(data.get("comparison"), "comparison").get("id"), "Comparison ID"
            )
            self._remember_poll_hint(
                resource_id,
                "comparison",
                data["comparison"].get("next_poll_after_ms"),
                retry_after,
            )
            return self._comparison(data, resource_id)
        except (TldwError, ValueError) as error:
            error.idempotency_key = stable_key
            raise

    def get_comparison(self, comparison_id, *, cancel_event=None):
        comparison_id = _identifier(comparison_id)
        data, _ = self._request(
            "GET", f"/v1/comparisons/{comparison_id}", cancel_event=cancel_event
        )
        return self._comparison(data, comparison_id)

    def wait_for_comparison(self, comparison_id, **options):
        return self._wait(_identifier(comparison_id), "comparison", **options)
