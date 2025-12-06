#!/usr/bin/env python3
"""
scrape_and_build.py - Modular DTSEN Data Scraper and Report Builder

This module provides a unified interface for scraping DTSEN data and generating
Excel/PDF reports. It uses a modular architecture with separate components for:
- API communication (APIClient)
- Data processing (DataProcessor)
- Excel generation (ReportGenerator)
- PDF generation (PDFBuilder)
- Visualization (Visualizer)
- Progress tracking (ProgressTracker)

The ScraperFacade orchestrates all components and provides backward compatibility
with the original InMemoryScraper interface.
"""

import warnings
from typing import List, Dict, Any, Callable

# Suppress warnings
warnings.filterwarnings("ignore")

# Import the modular scraper components
from scraper import (
    APIClient,
    DataProcessor,
    ReportGenerator,
    PDFBuilder,
    Visualizer,
    ProgressTracker,
    ScraperFacade
)

# Import constants for backward compatibility
from scraper.constants import (
    URL_FAMILY, URL_MEMBERS, URL_KYC, URL_PBI, URL_PKH, URL_BPNT,
    URL_ASET, URL_ASET_BERGERAK, ENDPOINTS,
    FAMILY_HEADERS, MEMBER_HEADERS, ASSET_IMMOVABLE, ASSET_MOVABLE,
    PAGE_SIZE_F4, THREADS_PER_PROCESS, SLEEP_BETWEEN_REQUESTS,
    RETRY_LIMIT, TIMEOUT
)

# Import core infrastructure for direct use
from core import (
    get_logger, LogContext,
    CircuitBreaker, validate_entity_lines,
    MemoryMonitor, DataOptimizer, cleanup_resources
)

# Re-export APIClient utilities for backward compatibility
from scraper.api_client import UnauthorizedError


# =============================================================================
# BACKWARD COMPATIBLE HELPER FUNCTIONS
# =============================================================================

def safe_b64decode(s: str) -> bytes:
    """Safely decode base64 string with padding fix."""
    s = (s or "").strip().replace("\n", "").replace(" ", "")
    s += "=" * (-len(s) % 4)
    import base64
    return base64.b64decode(s)


def decrypt_entity(entity_b64: str, aes_key_b64: str) -> Any:
    """Decrypt API response entity."""
    import json
    import hmac
    import hashlib
    import base64
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad

    outer = json.loads(safe_b64decode(entity_b64).decode("utf-8"))
    iv = safe_b64decode(outer["iv"])
    ciphertext = safe_b64decode(outer["value"])
    mac_expected = outer.get("mac")
    key = safe_b64decode(aes_key_b64)

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


def encrypt_entity_payload(payload_obj: Any, aes_key_b64: str) -> str:
    """Encrypt payload for API request."""
    import json
    import hmac
    import hashlib
    import base64
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    from Crypto.Random import get_random_bytes

    key = safe_b64decode(aes_key_b64)
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


# =============================================================================
# BACKWARD COMPATIBLE STANDALONE FUNCTIONS
# =============================================================================

def safe_post(url: str, data: Dict, headers: Dict, timeout: int = TIMEOUT) -> Any:
    """Standalone POST function for backward compatibility."""
    try:
        import httpx
        with httpx.Client(timeout=timeout) as client:
            return client.post(url, data=data, headers=headers)
    except ImportError:
        import requests
        return requests.post(url, data=data, headers=headers, timeout=timeout)


async def async_post(url: str, data: Dict, headers: Dict,
                     client: "httpx.AsyncClient", timeout: int = TIMEOUT) -> Any:
    """Async POST function for backward compatibility."""
    import httpx
    return await client.post(url, data=data, headers=headers, timeout=timeout)


# =============================================================================
# MAIN CLASS - InMemoryScraper (Facade for backward compatibility)
# =============================================================================

class InMemoryScraper(ScraperFacade):
    """
    In-memory scraper that fetches DTSEN data and builds reports.

    This class is now a thin wrapper around ScraperFacade, maintaining
    backward compatibility with the original interface while using the
    new modular architecture internally.

    Usage:
        scraper = InMemoryScraper(bearer_token, aes_key, progress_callback)
        result = scraper.run_full_pipeline(entity_lines)

        # Access results
        files = result['files']
        xlsx = files['Rekapitulasi.xlsx']
        pdf = files['FULL_REPORT.pdf']
    """

    def __init__(self, bearer_token: str, aes_key_b64: str,
                 progress_callback: Callable[[str, int, int], None] = None):
        """
        Initialize the scraper.

        Args:
            bearer_token: Authorization token for API calls
            aes_key_b64: Base64-encoded AES key for encryption/decryption
            progress_callback: Optional callback for progress updates
        """
        super().__init__(bearer_token, aes_key_b64, progress_callback)
        self._logger = get_logger('in_memory_scraper')
        self._logger.info("InMemoryScraper initialized (using modular architecture)")

    # Legacy method names for backward compatibility

    def _update_progress(self, stage: str, current: int, total: int):
        """Update progress via tracker."""
        self.progress_tracker.update(stage, current, total)

    def _post(self, url: str, data: Dict) -> Any:
        """Legacy POST method."""
        return self.api_client.post(url, data)

    def _jitter_sleep(self, label: str):
        """Legacy jitter sleep method."""
        self.api_client.jitter_sleep(label)


# =============================================================================
# MODULE EXPORTS
# =============================================================================

__all__ = [
    # Main classes
    'InMemoryScraper',
    'ScraperFacade',

    # Component classes
    'APIClient',
    'DataProcessor',
    'ReportGenerator',
    'PDFBuilder',
    'Visualizer',
    'ProgressTracker',

    # Helper functions
    'safe_post',
    'async_post',
    'decrypt_entity',
    'encrypt_entity_payload',
    'safe_b64decode',

    # Exceptions
    'UnauthorizedError',

    # Constants
    'URL_FAMILY',
    'URL_MEMBERS',
    'URL_KYC',
    'URL_PBI',
    'URL_PKH',
    'URL_BPNT',
    'URL_ASET',
    'URL_ASET_BERGERAK',
    'ENDPOINTS',
]


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # Test/demo mode
    print("scrape_and_build.py - Modular DTSEN Scraper")
    print("=" * 50)
    print("Components loaded:")
    print(f"  - APIClient: HTTP communication with encryption")
    print(f"  - DataProcessor: Data cleaning and transformation")
    print(f"  - ReportGenerator: Excel workbook generation")
    print(f"  - PDFBuilder: PDF report generation")
    print(f"  - Visualizer: Chart creation with fallbacks")
    print(f"  - ProgressTracker: Progress and ETA tracking")
    print(f"  - ScraperFacade: Component orchestration")
    print()
    print("Usage:")
    print("  from scrape_and_build import InMemoryScraper")
    print("  scraper = InMemoryScraper(token, aes_key, callback)")
    print("  result = scraper.run_full_pipeline(entity_lines)")
