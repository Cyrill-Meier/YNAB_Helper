"""Bounded, evicting sliding-window rate limiter (finding F87).

v1's limiter for ``/auth`` kept one deque per client IP forever: its clean-up only removed
*empty* deques, which never existed, so memory grew with every new address. This one

* keeps at most ``max_keys`` keys (least recently seen are evicted first),
* drops every key whose newest attempt is older than the window, on each call,
* can take back an attempt that did no work (:meth:`SlidingWindowLimiter.release`).

There is deliberately no cap over *all* keys together: login tokens carry 256 bits, so a
global cap would add no brute-force protection, while a flood from rotating addresses
could use it to lock every legitimate user out of sign-in. :func:`client_key` groups an
IPv6 client by its /64 (one host usually owns a whole /64) so rotating addresses inside
it does not give an attacker fresh budgets.

It is thread-safe (handlers run in the threadpool) and takes an injectable clock.
"""

from __future__ import annotations

import ipaddress
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable

__all__ = ["SlidingWindowLimiter", "client_key"]

#: IPv6 clients are limited per prefix of this length.
IPV6_CLIENT_PREFIX = 64


def client_key(ip: str | None) -> str:
    """The rate-limit key of a client address.

    IPv4 addresses are used as they are; an IPv4-mapped IPv6 address counts as its IPv4
    address; any other IPv6 address is reduced to its /64 network. Anything unparsable is
    used verbatim (capped), ``None`` becomes ``"unknown"``.
    """
    if not ip:
        return "unknown"
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return ip[:64]
    if isinstance(addr, ipaddress.IPv6Address):
        if addr.ipv4_mapped is not None:
            return str(addr.ipv4_mapped)
        return str(ipaddress.ip_network(f"{addr}/{IPV6_CLIENT_PREFIX}", strict=False))
    return str(addr)


class SlidingWindowLimiter:
    """At most ``limit`` attempts per key within ``window`` seconds.

    ``allow(key)`` records an attempt and returns True, or returns False (recording
    nothing) when the key is over the limit.
    """

    def __init__(
        self,
        limit: int,
        window: float = 60.0,
        *,
        max_keys: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if window <= 0:
            raise ValueError("window must be positive")
        if max_keys < 1:
            raise ValueError("max_keys must be at least 1")
        self.limit = int(limit)
        self.window = float(window)
        self.max_keys = int(max_keys)
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: OrderedDict[str, deque[float]] = OrderedDict()

    def allow(self, key: str) -> bool:
        """Record an attempt for ``key``; False if it is over the limit (nothing recorded)."""
        now = self._clock()
        cutoff = now - self.window
        with self._lock:
            self._evict_stale(cutoff)
            bucket = self._buckets.get(key)
            if bucket is not None:
                while bucket and bucket[0] <= cutoff:
                    bucket.popleft()
                self._buckets.move_to_end(key)
                if len(bucket) >= self.limit:
                    return False
            else:
                while len(self._buckets) >= self.max_keys:
                    self._buckets.popitem(last=False)
                bucket = deque()
                self._buckets[key] = bucket
            bucket.append(now)
            return True

    def release(self, key: str) -> None:
        """Take back the newest attempt of ``key`` (the work it allowed never happened)."""
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket:
                bucket.pop()
            if bucket is not None and not bucket:
                del self._buckets[key]

    def retry_after(self, key: str) -> float:
        """Seconds until ``key`` may try again (0 if it may now)."""
        now = self._clock()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None or len(bucket) < self.limit:
                return 0.0
            return max(0.0, bucket[len(bucket) - self.limit] + self.window - now)

    def __len__(self) -> int:
        """Number of keys currently tracked (bounded by ``max_keys``)."""
        with self._lock:
            return len(self._buckets)

    def clear(self) -> None:
        with self._lock:
            self._buckets.clear()

    def _evict_stale(self, cutoff: float) -> None:
        # Keys are ordered by last access; the front holds the least recently seen. A key
        # whose newest attempt is outside the window has nothing left to limit.
        while self._buckets:
            key, bucket = next(iter(self._buckets.items()))
            if bucket and bucket[-1] > cutoff:
                break
            del self._buckets[key]
