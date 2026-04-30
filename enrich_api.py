import asyncio
import logging
import os
import random
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import cloudscraper
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, ReadTimeout, RequestException
from urllib3.util.retry import Retry

_logger = logging.getLogger(__name__)

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(levelname)s:%(name)s:%(message)s",
    )

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

_DDG_HTML_URL = os.getenv("DDG_HTML_URL", "https://html.duckduckgo.com/html/")
_DDG_CONNECT_TIMEOUT = float(os.getenv("DDG_CONNECT_TIMEOUT", "8"))
_DDG_READ_TIMEOUT = float(os.getenv("DDG_READ_TIMEOUT", "20"))
_DDG_MAX_RETRIES = int(os.getenv("DDG_MAX_RETRIES", "3"))
_DDG_PRE_DELAY_MIN = float(os.getenv("DDG_PRE_DELAY_MIN", "1"))
_DDG_PRE_DELAY_MAX = float(os.getenv("DDG_PRE_DELAY_MAX", "3"))
_DDG_RETRY_DELAY_MIN = float(os.getenv("DDG_RETRY_DELAY_MIN", "4"))
_DDG_RETRY_DELAY_MAX = float(os.getenv("DDG_RETRY_DELAY_MAX", "9"))
_ENRICH_DELAY_MIN = float(os.getenv("ENRICH_DELAY_MIN", "25"))
_ENRICH_DELAY_MAX = float(os.getenv("ENRICH_DELAY_MAX", "45"))
_USE_CLOUDSCRAPER_FALLBACK = os.getenv("USE_CLOUDSCRAPER_FALLBACK", "true").lower() == "true"
_MIN_RESULT_SCORE = int(os.getenv("DDG_MIN_RESULT_SCORE", "60"))

_thread_local = threading.local()


def _build_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _mount_adapters(session: requests.Session) -> None:
    retry = Retry(
        total=0,
        connect=0,
        read=0,
        redirect=0,
        status=0,
        backoff_factor=0,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)


def _make_requests_session() -> requests.Session:
    session = requests.Session()
    _mount_adapters(session)
    session.headers.update(_build_headers())
    return session


def _make_cloudscraper_session() -> requests.Session:
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    _mount_adapters(session)
    session.headers.update(_build_headers())
    return session


def _get_requests_session() -> requests.Session:
    session = getattr(_thread_local, "requests_session", None)
    if session is None:
        session = _make_requests_session()
        _thread_local.requests_session = session
    return session


def _get_cloudscraper_session() -> requests.Session:
    session = getattr(_thread_local, "cloudscraper_session", None)
    if session is None:
        session = _make_cloudscraper_session()
        _thread_local.cloudscraper_session = session
    return session


def _normalize_domain(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("https://", "").replace("http://", "")
    value = value.replace("www.", "").strip("/")
    return value


def _source_domain_variants(source: str) -> List[str]:
    cleaned = _normalize_domain(source)
    variants = set()

    if cleaned:
        variants.add(cleaned)

    if cleaned and "." not in cleaned:
        variants.add(f"{cleaned}.com")

    alias_map = {
        "zillow": {"zillow.com"},
        "zillow.com": {"zillow.com"},
        "realtor": {"realtor.com"},
        "realtor.com": {"realtor.com"},
        "redfin": {"redfin.com"},
        "redfin.com": {"redfin.com"},
        "trulia": {"trulia.com"},
        "trulia.com": {"trulia.com"},
        "homes": {"homes.com"},
        "homes.com": {"homes.com"},
    }

    for variant in list(variants):
        variants.update(alias_map.get(variant, set()))

    return sorted({v.replace("www.", "") for v in variants if v})


def _tokenize_address(address: str) -> List[str]:
    tokens = re.findall(r"[a-z0-9]+", (address or "").lower())
    ignore = {
        "st", "street", "ave", "avenue", "rd", "road", "dr", "drive", "ln", "lane",
        "ct", "court", "ter", "terrace", "blvd", "boulevard", "cir", "circle",
        "port", "charlotte", "fl"
    }
    return [t for t in tokens if (t.isdigit() or len(t) >= 4) and t not in ignore]


def _extract_actual_url(raw_href: Optional[str]) -> Optional[str]:
    if not raw_href:
        return None

    full_url = urljoin("https://duckduckgo.com", raw_href)
    parsed = urlparse(full_url)

    if "duckduckgo.com" in parsed.netloc:
        unwrapped = parse_qs(parsed.query).get("uddg", [None])[0]
        if unwrapped:
            return unwrapped

    if parsed.scheme in ("http", "https"):
        return full_url

    return None


def _parse_results(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []

    for anchor in soup.select("a.result__a"):
        actual_url = _extract_actual_url(anchor.get("href"))
        if not actual_url:
            continue

        title = anchor.get_text(" ", strip=True)
        snippet = ""

        node = anchor
        for _ in range(4):
            node = node.parent
            if node is None:
                break
            snippet_node = node.select_one(".result__snippet")
            if snippet_node:
                snippet = snippet_node.get_text(" ", strip=True)
                break

        items.append({
            "url": actual_url,
            "title": title,
            "snippet": snippet,
        })

    return items


def _score_candidate(actual_url: str, title: str, snippet: str, address: str, source: str) -> int:
    parsed = urlparse(actual_url)
    domain = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.lower()

    score = 0
    source_domains = _source_domain_variants(source)

    if domain in source_domains:
        score += 70
    elif any(domain.endswith("." + s) for s in source_domains):
        score += 50
    elif any(s in domain for s in source_domains):
        score += 25
    else:
        score -= 80

    combined = f"{actual_url.lower()} {title.lower()} {snippet.lower()}"
    address_tokens = _tokenize_address(address)
    token_hits = sum(1 for token in address_tokens if token in combined)
    score += min(token_hits * 6, 30)

    house_number = next((t for t in address_tokens if t.isdigit()), None)
    if house_number and house_number in path:
        score += 12

    propertyish_paths = [
        "/homedetails/",
        "/realestateandhomes-detail/",
        "/property/",
        "/listing/",
    ]
    if any(marker in path for marker in propertyish_paths):
        score += 10

    bad_paths = [
        "/search",
        "/agent",
        "/agents",
        "/directory",
        "/profile",
        "/sitemap",
    ]
    if any(marker in path for marker in bad_paths):
        score -= 25

    if path in ("", "/"):
        score -= 20

    return score


def _request_ddg(
    client: requests.Session,
    client_name: str,
    url: str,
    address: str,
    attempt: int,
) -> requests.Response:
    headers = _build_headers()
    jitter = random.uniform(_DDG_PRE_DELAY_MIN, _DDG_PRE_DELAY_MAX)

    _logger.info(
        "DDG request start | address=%s | attempt=%s | client=%s | jitter=%.1fs",
        address,
        attempt,
        client_name,
        jitter,
    )

    time.sleep(jitter)

    response = client.get(
        url,
        headers=headers,
        timeout=(_DDG_CONNECT_TIMEOUT, _DDG_READ_TIMEOUT),
        allow_redirects=True,
    )

    _logger.info(
        "DDG response | address=%s | attempt=%s | client=%s | status=%s",
        address,
        attempt,
        client_name,
        response.status_code,
    )
    return response


def _ddg_fetch_url_for_property(address: str, source: str) -> Optional[str]:
    address = (address or "").strip()
    source = (source or "").strip()

    if not address or not source:
        return None

    query = quote_plus(f"{address} {source}")
    search_url = f"{_DDG_HTML_URL}?q={query}"
    last_error: Optional[str] = None

    for attempt in range(1, _DDG_MAX_RETRIES + 1):
        clients: List[Tuple[str, requests.Session]] = [("requests", _get_requests_session())]
        if _USE_CLOUDSCRAPER_FALLBACK:
            clients.append(("cloudscraper", _get_cloudscraper_session()))

        for client_name, client in clients:
            try:
                response = _request_ddg(client, client_name, search_url, address, attempt)

                if response.status_code in (202, 403, 429):
                    last_error = f"challenge-{response.status_code}"
                    wait = random.uniform(_DDG_RETRY_DELAY_MIN, _DDG_RETRY_DELAY_MAX) * attempt
                    _logger.warning(
                        "DDG challenge | address=%s | attempt=%s | client=%s | status=%s | backoff=%.1fs",
                        address,
                        attempt,
                        client_name,
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    last_error = f"http-{response.status_code}"
                    wait = random.uniform(_DDG_RETRY_DELAY_MIN, _DDG_RETRY_DELAY_MAX)
                    _logger.warning(
                        "DDG non-200 | address=%s | attempt=%s | client=%s | status=%s | backoff=%.1fs",
                        address,
                        attempt,
                        client_name,
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                candidates = _parse_results(response.text)
                _logger.info(
                    "DDG parsed candidates | address=%s | attempt=%s | client=%s | count=%s",
                    address,
                    attempt,
                    client_name,
                    len(candidates),
                )

                if not candidates:
                    last_error = "no-results"
                    continue

                scored = []
                for candidate in candidates:
                    score = _score_candidate(
                        candidate["url"],
                        candidate["title"],
                        candidate["snippet"],
                        address,
                        source,
                    )
                    scored.append((score, candidate["url"]))

                scored.sort(key=lambda x: x[0], reverse=True)
                best_score, best_url = scored[0]

                _logger.info(
                    "DDG best candidate | address=%s | attempt=%s | client=%s | score=%s | url=%s",
                    address,
                    attempt,
                    client_name,
                    best_score,
                    best_url,
                )

                if best_score >= _MIN_RESULT_SCORE:
                    return best_url

                last_error = f"low-score-{best_score}"

            except ConnectTimeout as exc:
                last_error = f"connect-timeout:{exc.__class__.__name__}"
                _logger.warning(
                    "DDG connect timeout | address=%s | attempt=%s | client=%s | error=%s",
                    address,
                    attempt,
                    client_name,
                    exc,
                )
            except ReadTimeout as exc:
                last_error = f"read-timeout:{exc.__class__.__name__}"
                _logger.warning(
                    "DDG read timeout | address=%s | attempt=%s | client=%s | error=%s",
                    address,
                    attempt,
                    client_name,
                    exc,
                )
            except RequestException as exc:
                last_error = f"request-exception:{exc.__class__.__name__}"
                _logger.warning(
                    "DDG request exception | address=%s | attempt=%s | client=%s | error=%s",
                    address,
                    attempt,
                    client_name,
                    exc,
                )
            except Exception as exc:
                last_error = f"unexpected:{exc.__class__.__name__}"
                _logger.exception(
                    "DDG unexpected error | address=%s | attempt=%s | client=%s | error=%s",
                    address,
                    attempt,
                    client_name,
                    exc,
                )

        if attempt < _DDG_MAX_RETRIES:
            wait = random.uniform(_DDG_RETRY_DELAY_MIN, _DDG_RETRY_DELAY_MAX) * attempt
            _logger.info(
                "DDG retrying | address=%s | attempt=%s | backoff=%.1fs | last_error=%s",
                address,
                attempt,
                wait,
                last_error,
            )
            time.sleep(wait)

    _logger.error(
        "DDG failed | address=%s | source=%s | last_error=%s",
        address,
        source,
        last_error,
    )
    return None


async def enrich(data: dict) -> dict:
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])
    results = []

    _logger.info("Starting batch enrichment for %d items", len(items))

    for i, item in enumerate(items):
        address = item.get("address")
        source = item.get("source")

        if not address or not source:
            results.append({**item, "url": None, "source_url": None})
            continue

        if i > 0:
            delay = random.uniform(_ENRICH_DELAY_MIN, _ENRICH_DELAY_MAX)
            _logger.info("Per-item delay before '%s': %.1fs", address, delay)
            await asyncio.sleep(delay)

        url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)

        results.append({
            **item,
            "url": url,
            "source_url": url,
        })

        if url:
            _logger.info("Enriched item %d: %s -> %s", i + 1, address, url)
        else:
            _logger.warning("Failed item %d: %s", i + 1, address)

    return {"results": results}