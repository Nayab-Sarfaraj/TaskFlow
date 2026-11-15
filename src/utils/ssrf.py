from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import List, Tuple
from urllib.parse import urljoin, urlparse


MAX_REDIRECTS = 5


def _is_blocked_address(address: ipaddress._BaseAddress) -> bool:
    return (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
        or address.is_unspecified
        or str(address) in {"169.254.169.254", "fd00:ec2::254"}
    )


async def resolve_safe_url(url: str) -> Tuple[str, str]:
    """Validate a URL and all currently resolved addresses before connecting."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("only HTTP and HTTPS URLs are allowed")
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("URL must contain a hostname and no credentials")

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        resolved = await asyncio.to_thread(
            socket.getaddrinfo,
            parsed.hostname,
            port,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ValueError(f"could not resolve URL hostname: {parsed.hostname}") from exc

    addresses: List[ipaddress._BaseAddress] = []
    for _, _, _, _, sockaddr in resolved:
        address = ipaddress.ip_address(sockaddr[0])
        if _is_blocked_address(address):
            raise ValueError(f"URL resolves to a blocked address: {address}")
        addresses.append(address)

    if not addresses:
        raise ValueError("URL did not resolve to a usable address")
    return url, str(addresses[0])


async def validate_safe_url(url: str) -> str:
    await resolve_safe_url(url)
    return url


async def validate_redirect(base_url: str, location: str) -> str:
    target = urljoin(base_url, location)
    return await validate_safe_url(target)
