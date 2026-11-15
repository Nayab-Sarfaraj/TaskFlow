from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from typing import Any, Dict, List

import httpx
import httpcore
from httpcore._backends.auto import AutoBackend
from pydantic import BaseModel, Field, HttpUrl

from src.models.job_attempts import JobAttempt
from src.models.jobs import Job, JobType
from src.utils.ssrf import MAX_REDIRECTS, resolve_safe_url, validate_redirect


logger = logging.getLogger(__name__)


SCRAPING_MAX_URLS = int(os.getenv("MAX_SCRAPING_URLS", "20"))
SCRAPING_MAX_RESPONSE_BYTES = int(os.getenv("MAX_HTTP_RESPONSE_MB", "10")) * 1024 * 1024
SCRAPING_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("SCRAPING_REQUEST_TIMEOUT_SECONDS", "15")
)


class ScrapingJobPayload(BaseModel):
    urls: List[HttpUrl] = Field(min_length=1, max_length=SCRAPING_MAX_URLS)


class PinnedNetworkBackend(httpcore.AsyncNetworkBackend):
    """Connect to the IP selected by SSRF validation, not a fresh DNS result."""

    def __init__(self) -> None:
        self._backend = AutoBackend()
        self._pinned: Dict[str, str] = {}

    def pin(self, hostname: str, address: str) -> None:
        self._pinned[hostname] = address

    async def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):
        return await self._backend.connect_tcp(
            self._pinned.get(host, host),
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):
        return await self._backend.connect_unix_socket(
            path, timeout=timeout, socket_options=socket_options
        )

    async def sleep(self, seconds):
        return await self._backend.sleep(seconds)


class PinnedAsyncHTTPTransport(httpx.AsyncHTTPTransport):
    def __init__(self, backend: PinnedNetworkBackend) -> None:
        super().__init__(trust_env=False)
        self.backend = backend
        self._pool = httpcore.AsyncConnectionPool(network_backend=backend)


def _extract_page_metadata(html: str) -> Dict[str, Any]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else None
    metadata = {
        name.lower(): content
        for name, content in re.findall(
            r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\']',
            html,
            re.I,
        )
    }
    return {"title": title, "metadata": metadata}


async def _fetch_page(
    client: httpx.AsyncClient,
    transport: PinnedAsyncHTTPTransport,
    url: HttpUrl,
) -> Dict[str, Any]:
    chunks: list[bytes] = []
    total_size = 0
    current_url = str(url)
    for _ in range(MAX_REDIRECTS + 1):
        current_url, validated_ip = await resolve_safe_url(current_url)
        hostname = httpx.URL(current_url).host
        transport.backend.pin(hostname, validated_ip)
        async with client.stream(
            "GET", current_url, follow_redirects=False
        ) as response:
            if response.is_redirect:
                location = response.headers.get("location")
                if not location:
                    raise ValueError(f"redirect from {current_url} has no location")
                current_url = await validate_redirect(current_url, location)
                continue

            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                total_size += len(chunk)
                if total_size > SCRAPING_MAX_RESPONSE_BYTES:
                    raise ValueError(
                        f"response from {current_url} exceeds the configured size limit"
                    )
                chunks.append(chunk)
            break
    else:
        raise ValueError("maximum redirect limit exceeded")

    page = _extract_page_metadata(b"".join(chunks).decode("utf-8", errors="replace"))
    return {
        "url_hash": hashlib.sha256(str(url).encode("utf-8")).hexdigest(),
        **page,
    }


async def process_scraping(job: Job, job_attempt: JobAttempt) -> Dict[str, Any]:
    """Fetch up to 20 URLs and extract basic title/meta information."""
    if job.job_type != JobType.SCRAPING:
        raise ValueError("process_scraping requires a SCRAPING job")

    payload = ScrapingJobPayload.model_validate(job.payload)
    timeout = httpx.Timeout(SCRAPING_REQUEST_TIMEOUT_SECONDS)
    pages = []
    transport = PinnedAsyncHTTPTransport(PinnedNetworkBackend())
    async with httpx.AsyncClient(
        transport=transport,
        timeout=timeout,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        for url in payload.urls:
            try:
                pages.append(
                    await asyncio.wait_for(
                        _fetch_page(client, transport, url),
                        timeout=SCRAPING_REQUEST_TIMEOUT_SECONDS,
                    )
                )
            except Exception as exc:
                logger.warning("Scraping URL failed: %s", type(exc).__name__)
                pages.append(
                    {
                        "url_hash": hashlib.sha256(
                            str(url).encode("utf-8")
                        ).hexdigest(),
                        "status": "failed",
                        "error": type(exc).__name__,
                    }
                )

    return {"pages": pages, "total": len(pages)}
