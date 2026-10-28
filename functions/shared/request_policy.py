"""Finite source retries and publish waits within the caller's invocation budget."""

import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

INVOCATION_SECONDS = 50
TRANSIENT_STATUS = {408, 429, 500, 502, 503, 504}


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise RuntimeError("Source invocation deadline exceeded")
    return seconds


def retry_delay(response, attempt):
    value = getattr(response, "headers", {}).get("Retry-After", "")
    if value:
        try:
            seconds = int(value)
            if seconds >= 0:
                return seconds
        except ValueError:
            try:
                instant = parsedate_to_datetime(value)
                if instant.tzinfo is not None:
                    return max(0, (instant - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
    return 0.5 * 2 ** attempt


def get(requests, url, *, deadline, label, **kwargs):
    """Retry transport errors and selected statuses at most three times.

    Never include response bodies, URLs or transport exceptions in errors. Requests
    connect/read limits are inactivity limits; the platform's 60s limit remains
    the final bound for DNS, slow streaming bodies and SDK worker threads.
    """
    for attempt in range(3):
        budget = remaining(deadline)
        response = None
        try:
            response = requests.get(
                url, timeout=(min(5, budget / 2), min(20, budget / 2)), **kwargs
            )
        except requests.RequestException:
            failure = f"{label} request failed"
        else:
            remaining(deadline)
            if response.status_code == 200:
                return response
            failure = f"{label} HTTP status {response.status_code}"
            if response.status_code not in TRANSIENT_STATUS:
                raise RuntimeError(failure) from None
        delay = retry_delay(response, attempt)
        if attempt == 2 or delay + 1 >= remaining(deadline):
            raise RuntimeError(failure) from None
        time.sleep(delay)


def publish_timeout(deadline):
    return min(10, remaining(deadline))
