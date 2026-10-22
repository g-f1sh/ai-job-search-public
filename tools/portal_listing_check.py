"""Bounded HTTP-only availability checks. Inaccessibility is never evidence of closure."""
from __future__ import annotations

import ipaddress
import json
import math
import os
import re
import socket
import time
from datetime import datetime
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener


class CheckStopped(Exception):
    pass


def public_url(url):
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Only public HTTP(S) listing URLs are allowed")
    for entry in socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM):
        if not ipaddress.ip_address(entry[4][0]).is_global:
            raise ValueError("Private/internal listing destinations are blocked")
    return url


class PublicRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return super().redirect_request(req, fp, code, msg, headers, public_url(newurl))


class ListingHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text, self.schemas, self.data = [], [], []
        self.script = False
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1
            self.script = tag == "script" and dict(attrs).get("type", "").lower() == "application/ld+json"
            self.data = []

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            if self.script:
                try:
                    self.schemas.append(json.loads("".join(self.data)))
                except (ValueError, TypeError):
                    pass
            self.script = False
            self.skip = max(0, self.skip - 1)

    def handle_data(self, data):
        if self.script:
            self.data.append(data)
        elif not self.skip:
            self.text.append(data)


def classify_listing(job, body):
    parser = ListingHTML()
    parser.feed(body)
    text = " ".join(" ".join(parser.text).split())
    lowered = text.casefold()
    def result(status, reason, **extra):
        return {"status": status, "reason": reason, "needs_ai": False, **extra}
    if any(x in lowered for x in ("verify you are human", "access denied", "checking your browser", "captcha", "sign in to view", "log in to view", "enable javascript", "javascript is required", "security verification")):
        return result("unknown", "Blocked, login-protected, or dynamic content unavailable")
    normalize = lambda s: " ".join(re.findall(r"\w+", s.casefold()))
    title = normalize(job["title"])
    matches_title = title and title in normalize(text)
    def flatten(value):
        if isinstance(value, list):
            for v in value:
                yield from flatten(v)
        elif isinstance(value, dict):
            yield value
            if "@graph" in value:
                yield from flatten(value["@graph"])
    for schema in flatten(parser.schemas):
        kinds = schema.get("@type", [])
        if isinstance(kinds, str):
            kinds = [kinds]
        if "JobPosting" not in kinds or normalize(str(schema.get("title", ""))) != title:
            continue
        valid = schema.get("validThrough")
        if valid:
            try:
                expiry = datetime.fromisoformat(str(valid).replace("Z", "+00:00"))
                if expiry.tzinfo is None:
                    expiry = expiry.astimezone()
                if expiry < datetime.now().astimezone():
                    return result("closed", "Matching JobPosting has an expired validThrough date")
            except ValueError:
                pass
        if schema.get("description") and schema.get("hiringOrganization"):
            return result("active", "Matching structured JobPosting with responsibilities and employer is publicly served")
    if matches_title and any(x in lowered for x in ("no longer accepting applications", "this job is no longer available", "this position has been filled", "this job has expired", "משרה זו אינה זמינה", "המשרה אינה רלוונטית")):
        return result("closed", "Matching posting explicitly says applications are closed")
    if matches_title and any(x in lowered for x in ("apply for this job", "apply for this position", "apply now", "הגש מועמדות", "הגשת מועמדות")):
        return result("active", "Matching posting title and explicit application action are visible")
    if matches_title and len(text) >= 500:
        # Only substantive accessible evidence benefits from interpretation. Never
        # spend inference on a timeout, login wall, 404, or empty JavaScript shell.
        pattern = r"\W+".join(re.escape(word) for word in re.findall(r"\w+", job["title"]))
        match = re.search(pattern, text, re.I)
        start = max(0, (match.start() if match else 0) - 500)
        return result("unknown", "Accessible posting needs interpretation", needs_ai=True, excerpt=text[start:start + 6000])
    return result("unknown", "No reliable matching posting/application evidence in fetched content")


class ListingChecker:
    def __init__(self, store, *, timeout=None, cache_seconds=None, rate_seconds=None):
        self.store = store
        def bounded(value, name, default, low, high):
            try:
                parsed = float(value if value is not None else os.environ.get(name, default))
                return min(high, max(low, parsed)) if math.isfinite(parsed) else default
            except ValueError:
                return default
        self.timeout = bounded(timeout, "PORTAL_REFRESH_HTTP_TIMEOUT", 8, 1, 30)
        self.cache_seconds = bounded(cache_seconds, "PORTAL_REFRESH_CACHE_SECONDS", 21600, 0, 86400)
        self.rate_seconds = bounded(rate_seconds, "PORTAL_REFRESH_RATE_SECONDS", 1, 0, 60)
        self.last_host = {}

    def check(self, job, stopped=lambda: False):
        if stopped():
            raise CheckStopped()
        url = job["source_url"]
        # Classification includes role identity, so cache must include job ID.
        cache_key = job["id"] + ":" + url
        with self.store._connect() as connection:
            cached = connection.execute("SELECT * FROM listing_http_cache WHERE url=?", (cache_key,)).fetchone()
        if cached and time.time() - cached["checked_at"] < self.cache_seconds:
            return {**json.loads(cached["result"]), "cached": True}
        host = urlsplit(url).hostname
        wait_until = self.last_host.get(host, 0) + self.rate_seconds
        while time.monotonic() < wait_until:
            if stopped():
                raise CheckStopped()
            time.sleep(min(.1, max(0, wait_until - time.monotonic())))
        self.last_host[host] = time.monotonic()
        try:
            public_url(url)
            deadline = time.monotonic() + self.timeout
            request = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; PrivateJobPortal/1.0)", "Accept": "text/html,application/json"})
            with build_opener(PublicRedirects()).open(request, timeout=self.timeout) as response:
                data = bytearray()
                while len(data) < 512_000:
                    if stopped():
                        raise CheckStopped()
                    if time.monotonic() > deadline:
                        raise TimeoutError("Listing response exceeded deadline")
                    chunk = response.read1(min(16384, 512_000 - len(data)))
                    if not chunk:
                        break
                    data.extend(chunk)
                body = data.decode(response.headers.get_content_charset() or "utf-8", errors="replace")
                result = classify_listing(job, body)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            result = {"status": "unknown", "needs_ai": False, "reason": f"HTTP check unavailable: {type(exc).__name__}: {str(exc)[:200]}"}
        if stopped():
            raise CheckStopped()
        with self.store._connect() as connection:
            connection.execute("INSERT OR REPLACE INTO listing_http_cache VALUES(?,?,?)", (cache_key, json.dumps(result), time.time()))
        return {**result, "cached": False}
