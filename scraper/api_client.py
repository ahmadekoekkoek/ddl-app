"""
API Client
Handles HTTP requests, encryption, authentication, and retry logic.
"""

import time
import json
import base64
import hmac
import hashlib
import asyncio
import random
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes

from core import (
    get_logger, CircuitBreaker,
    APIError, AuthenticationError, RateLimitError
)
from .constants import (
    TIMEOUT, RETRY_LIMIT, ENDPOINT_TUNING, THREADS_PER_PROCESS,
    SLEEP_BETWEEN_REQUESTS
)

# Check for httpx
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    import requests


class UnauthorizedError(AuthenticationError):
    """Raised when API responds with HTTP 401."""
    def __init__(self, message: str = "HTTP 401 Unauthorized"):
        super().__init__(message, status_code=401)


class APIClient:
    """Handles all HTTP communication with encryption support."""

    def __init__(self, bearer_token: str, aes_key_b64: str):
        self.token = bearer_token
        self.aes_key = aes_key_b64
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Authorization": bearer_token,
            "Origin": "https://siks.kemensos.go.id",
            "Referer": "https://siks.kemensos.go.id/",
        }

        self._logger = get_logger('api_client')
        self._circuit = CircuitBreaker(name="api", failure_threshold=5, recovery_timeout=300)

        # Initialize HTTP client
        self.client = None
        self._init_client()

    def _init_client(self):
        """Initialize persistent HTTP client."""
        try:
            if HTTPX_AVAILABLE:
                limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
                self.client = httpx.Client(
                    headers=self.headers,
                    timeout=TIMEOUT,
                    http2=True,
                    limits=limits
                )
            else:
                self.client = requests.Session()
                self.client.headers.update(self.headers)
            self._logger.debug("HTTP client initialized")
        except Exception as e:
            self.client = None
            self._logger.warning(f"Failed to initialize HTTP client: {e}")

    def close(self):
        """Close HTTP client."""
        try:
            if self.client is None:
                return
            if HTTPX_AVAILABLE and isinstance(self.client, httpx.Client):
                self.client.close()
            elif hasattr(self.client, 'close'):
                self.client.close()
            self._logger.debug("HTTP client closed")
        except Exception as e:
            self._logger.warning(f"Client close error: {e}")

    # Encryption methods

    @staticmethod
    def safe_b64decode(s: str) -> bytes:
        """Safely decode base64 string."""
        s = (s or "").strip().replace("\n", "").replace(" ", "")
        s += "=" * (-len(s) % 4)
        return base64.b64decode(s)

    def decrypt_entity(self, entity_b64: str) -> Any:
        """Decrypt API response entity."""
        outer = json.loads(self.safe_b64decode(entity_b64).decode("utf-8"))
        iv = self.safe_b64decode(outer["iv"])
        ciphertext = self.safe_b64decode(outer["value"])
        mac_expected = outer.get("mac")
        key = self.safe_b64decode(self.aes_key)

        # Verify MAC
        mac_data = base64.b64encode(iv).decode() + base64.b64encode(ciphertext).decode()
        mac_calc = hmac.new(key, mac_data.encode(), hashlib.sha256).hexdigest()
        if mac_expected and mac_calc != mac_expected:
            raise ValueError("MAC mismatch")

        # Decrypt
        cipher = AES.new(key, AES.MODE_CBC, iv)
        plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)

        try:
            return json.loads(plaintext.decode("utf-8"))
        except:
            return plaintext.decode("utf-8")

    def encrypt_payload(self, payload_obj: Any) -> str:
        """Encrypt payload for API request."""
        key = self.safe_b64decode(self.aes_key)
        iv = get_random_bytes(16)
        cipher = AES.new(key, AES.MODE_CBC, iv)

        plaintext = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))

        mac_data = base64.b64encode(iv).decode() + base64.b64encode(ciphertext).decode()
        mac = hmac.new(key, mac_data.encode(), hashlib.sha256).hexdigest()

        outer = {
            "iv": base64.b64encode(iv).decode(),
            "value": base64.b64encode(ciphertext).decode(),
            "mac": mac
        }
        return base64.b64encode(json.dumps(outer, separators=(",", ":")).encode()).decode()

    # HTTP methods

    def post(self, url: str, data: Dict) -> Optional[Any]:
        """POST with circuit breaker and retry logic."""
        if not self._circuit.is_available():
            self._logger.warning(f"Circuit breaker OPEN - skipping {url}")
            return None

        backoff = 1.0
        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                start = time.perf_counter()

                if self.client is None:
                    return self._fallback_post(url, data)

                if HTTPX_AVAILABLE and isinstance(self.client, httpx.Client):
                    r = self.client.post(url, data=data)
                else:
                    r = self.client.post(url, data=data, timeout=TIMEOUT)

                duration = (time.perf_counter() - start) * 1000
                status = getattr(r, 'status_code', None)

                if status == 200:
                    self._circuit.record_success()
                    self._logger.debug(f"POST {url} -> 200 ({duration:.1f}ms)")
                    return r

                if status == 401:
                    self._logger.error("401 Unauthorized")
                    self._circuit.record_failure(UnauthorizedError())
                    raise UnauthorizedError()

                if status == 429:
                    retry_after = getattr(r, 'headers', {}).get("Retry-After")
                    delay = float(retry_after) if retry_after and str(retry_after).isdigit() else backoff
                    self._logger.warning(f"429 Rate Limited - sleeping {delay:.2f}s")
                    time.sleep(delay)
                    backoff *= 1.5
                    continue

                self._logger.warning(f"HTTP {status}: {r.text[:200] if hasattr(r, 'text') else ''}")
                self._circuit.record_failure(APIError(f"HTTP {status}", status_code=status))

            except UnauthorizedError:
                raise
            except Exception as e:
                self._logger.error(f"HTTP error (attempt {attempt}/{RETRY_LIMIT}): {e}")
                self._circuit.record_failure(e)

            time.sleep(backoff)
            backoff *= 1.8

        return None

    def _fallback_post(self, url: str, data: Dict) -> Optional[Any]:
        """Fallback POST without persistent client."""
        try:
            if HTTPX_AVAILABLE:
                with httpx.Client(timeout=TIMEOUT) as client:
                    return client.post(url, data=data, headers=self.headers)
            else:
                return requests.post(url, data=data, headers=self.headers, timeout=TIMEOUT)
        except Exception as e:
            self._logger.error(f"Fallback POST failed: {e}")
            return None

    async def async_post(self, url: str, data: Dict, client: "httpx.AsyncClient") -> Optional[Any]:
        """Async POST with retry logic."""
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx required for async operations")

        backoff = 1.0
        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                start = time.perf_counter()
                r = await client.post(url, data=data, headers=self.headers, timeout=TIMEOUT)
                duration = (time.perf_counter() - start) * 1000

                if r.status_code == 200:
                    self._logger.debug(f"POST {url} -> 200 ({duration:.1f}ms)")
                    return r

                if r.status_code == 401:
                    raise UnauthorizedError()

                if r.status_code == 429:
                    delay = float(r.headers.get("Retry-After", backoff))
                    await asyncio.sleep(delay)
                    backoff *= 1.5
                    continue

                self._logger.warning(f"HTTP {r.status_code}")

            except UnauthorizedError:
                raise
            except Exception as e:
                self._logger.error(f"Async error (attempt {attempt}): {e}")

            await asyncio.sleep(backoff)
            backoff *= 1.8

        return None

    # High-level fetch methods

    def jitter_sleep(self, label: str):
        """Sleep with jitter based on endpoint."""
        tuning = ENDPOINT_TUNING.get(label.upper(), ENDPOINT_TUNING["DEFAULT"])
        lo, hi = tuning.get("sleep_range", (SLEEP_BETWEEN_REQUESTS, SLEEP_BETWEEN_REQUESTS + 0.1))
        time.sleep(random.uniform(lo, hi))

    def fetch_families(self, url: str, entity_lines: List[str],
                       progress_cb=None) -> List[Dict]:
        """Fetch family data for all entities."""
        self._logger.info(f"Fetching families for {len(entity_lines)} entities")
        families = []

        for idx, entity in enumerate(entity_lines, 1):
            if progress_cb:
                progress_cb("Fetching families", idx, len(entity_lines))

            r = self.post(url, {"entity": entity})
            if not r:
                continue

            try:
                resp_json = r.json()
                enc = resp_json.get("data") if isinstance(resp_json, dict) else r.text.strip('"')
                if not enc:
                    continue

                dec = self.decrypt_entity(enc)
                rows = dec.get("data", []) if isinstance(dec, dict) else []

                for row in rows:
                    idk = row.get("id_keluarga") or row.get("ID_KELUARGA")
                    row["id_keluarga_parent"] = idk

                families.extend(rows)

            except Exception as e:
                self._logger.error(f"Family processing error: {e}")

            self.jitter_sleep("FAMILIES")

        self._logger.info(f"Fetched {len(families)} families")
        return families

    def fetch_endpoint(self, label: str, url: str, id_list: List[str],
                       progress_cb=None) -> List[Dict]:
        """Fetch data for endpoint with concurrent workers."""
        results = []

        def fetch_one(id_keluarga: str) -> List[Dict]:
            try:
                entity = self.encrypt_payload({"id_keluarga": id_keluarga})
            except Exception as e:
                self._logger.error(f"[{label}] Encrypt failed: {e}")
                return []

            r = self.post(url, {"entity": entity})
            if not r:
                return []

            try:
                resp_json = r.json()
                enc = resp_json.get("data") if isinstance(resp_json, dict) and resp_json.get("status") else r.text.strip().strip('"')
                if not enc:
                    return []

                decrypted = self.decrypt_entity(enc)

                if isinstance(decrypted, dict) and "data" in decrypted:
                    extracted = decrypted["data"] or []
                elif isinstance(decrypted, list):
                    extracted = decrypted
                else:
                    extracted = [decrypted] if isinstance(decrypted, dict) else []

                for item in extracted:
                    if isinstance(item, dict):
                        item["id_keluarga_parent"] = id_keluarga

                self.jitter_sleep(label)
                return extracted

            except Exception as e:
                self._logger.error(f"[{label}] Decrypt failed: {e}")
                return []

        # Get worker count from tuning
        tuning = ENDPOINT_TUNING.get(label.upper(), ENDPOINT_TUNING["DEFAULT"])
        max_workers = tuning.get("workers", THREADS_PER_PROCESS)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fetch_one, idk): idk for idk in id_list}

            for idx, future in enumerate(as_completed(futures), 1):
                if progress_cb:
                    progress_cb(f"Fetching {label}", idx, len(id_list))

                try:
                    rows = future.result()
                    results.extend(rows)
                except UnauthorizedError:
                    raise
                except Exception as e:
                    self._logger.error(f"[{label}] Worker error: {e}")

        return results

    async def fetch_endpoint_async(self, label: str, url: str, id_list: List[str],
                                   progress_cb=None) -> List[Dict]:
        """Async version of fetch_endpoint with shared client for performance."""
        if not HTTPX_AVAILABLE:
            return self.fetch_endpoint(label, url, id_list, progress_cb)

        tuning = ENDPOINT_TUNING.get(label.upper(), ENDPOINT_TUNING["DEFAULT"])
        max_workers = tuning.get("workers", THREADS_PER_PROCESS)

        results = []
        sem = asyncio.Semaphore(max_workers)
        idx_counter = [0]

        # Create SHARED client OUTSIDE workers for connection pooling
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        async with httpx.AsyncClient(
            headers=self.headers,
            timeout=TIMEOUT,
            http2=True,
            limits=limits
        ) as client:

            async def worker(id_keluarga: str) -> List[Dict]:
                async with sem:
                    try:
                        entity = self.encrypt_payload({"id_keluarga": id_keluarga})
                    except Exception as e:
                        return []

                    # Use shared client - no new connection overhead!
                    r = await self.async_post(url, {"entity": entity}, client)

                    if not r:
                        return []

                    try:
                        resp_json = r.json()
                        enc = resp_json.get("data") if isinstance(resp_json, dict) and resp_json.get("status") else r.text.strip().strip('"')

                        if not enc:
                            return []

                        decrypted = self.decrypt_entity(enc)

                        if isinstance(decrypted, dict) and "data" in decrypted:
                            extracted = decrypted["data"] or []
                        elif isinstance(decrypted, list):
                            extracted = decrypted
                        else:
                            extracted = [decrypted] if isinstance(decrypted, dict) else []

                        for item in extracted:
                            if isinstance(item, dict):
                                item["id_keluarga_parent"] = id_keluarga

                        return extracted

                    except Exception as e:
                        self._logger.error(f"[{label}] Async decrypt failed: {e}")
                        return []
                    finally:
                        idx_counter[0] += 1
                        if progress_cb:
                            progress_cb(f"Fetching {label}", idx_counter[0], len(id_list))

            tasks = [asyncio.create_task(worker(i)) for i in id_list]

            for coro in asyncio.as_completed(tasks):
                rows = await coro
                results.extend(rows)

        return results
