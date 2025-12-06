"""Reusable Selenium-based auto capture session for SIKS credentials."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class AutoCaptureSession:
    """Manages a long-running Chrome session that continuously captures creds."""

    def __init__(self, siks_url: str = "https://siks.kemensos.go.id"):
        self.siks_url = siks_url
        self.driver: Optional[webdriver.Chrome] = None
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._processed_request_ids: set[str] = set()
        self._bearer_token: Optional[str] = None
        self._entity_payloads: List[str] = []
        self._api_endpoint: Optional[str] = None
        self._token_event = threading.Event()
        self._entity_event = threading.Event()

    # ------------------------------------------------------------------ driver
    def start(self):
        """Launch Chrome with required logging capabilities."""
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--force-device-scale-factor=1")
        options.add_argument("--high-dpi-support=1")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--lang=id-ID")
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        options.add_experimental_option("prefs", {"intl.accept_languages": "id-ID,id"})
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })
        self.driver.execute_cdp_cmd("Network.enable", {})
        self.driver.execute_cdp_cmd("Page.enable", {})

    def open_portal(self):
        if not self.driver:
            raise RuntimeError("Driver not started")
        self.driver.get(self.siks_url)

    def start_monitoring(self):
        if self._monitoring:
            return
        if not self.driver:
            raise RuntimeError("Driver not started")
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _monitor_loop(self):
        assert self.driver is not None
        while self._monitoring:
            try:
                logs = self.driver.get_log("performance")
            except Exception:
                time.sleep(0.5)
                continue

            for entry in logs:
                try:
                    message = json.loads(entry["message"]).get("message", {})
                except json.JSONDecodeError:
                    continue

                if message.get("method") != "Network.requestWillBeSent":
                    continue

                params = message.get("params", {})
                request = params.get("request", {})
                headers = request.get("headers", {})

                auth_header = headers.get("authorization") or headers.get("Authorization")
                if auth_header:
                    token = auth_header.replace("Bearer", "").strip()
                    if token:
                        self._store_bearer_token(token)

                url = request.get("url", "")
                method = request.get("method")
                if method != "POST" or "get-keluarga-dtsen" not in url:
                    continue

                request_id = params.get("requestId")
                dedupe_key = request_id or f"{url}:{params.get('timestamp')}"
                if dedupe_key in self._processed_request_ids:
                    continue
                self._processed_request_ids.add(dedupe_key)

                post_data = request.get("postData")
                if not post_data and request_id:
                    try:
                        payload_result = self.driver.execute_cdp_cmd(
                            "Network.getRequestPostData", {"requestId": request_id}
                        )
                        post_data = payload_result.get("postData")
                    except Exception:
                        post_data = None

                entity_value = self._parse_entity_from_post_data(post_data)
                if entity_value:
                    self._store_entity_payload(entity_value, url)

            time.sleep(0.4)

    # ----------------------------------------------------------------- helpers
    def _store_bearer_token(self, token: str):
        with self._lock:
            if self._bearer_token == token:
                return
            self._bearer_token = token
            self._token_event.set()

    def _store_entity_payload(self, payload: str, url: str):
        with self._lock:
            if payload in self._entity_payloads:
                return
            self._entity_payloads.append(payload)
            self._api_endpoint = url
            self._entity_event.set()

    @staticmethod
    def _parse_entity_from_post_data(post_data: Optional[str]) -> Optional[str]:
        if not post_data:
            return None

        decoded = post_data
        if "entity=" in decoded:
            fragment = decoded.split("entity=", 1)[1]
            for delimiter in ["\r\n", "&"]:
                idx = fragment.find(delimiter)
                if idx != -1:
                    fragment = fragment[:idx]
            return fragment.strip() or None

        try:
            body = json.loads(decoded)
        except json.JSONDecodeError:
            body = None
        if isinstance(body, dict):
            entity_value = body.get("entity")
            if entity_value:
                return entity_value

        lines = decoded.splitlines()
        for idx, line in enumerate(lines):
            if 'name="entity"' in line and idx + 2 < len(lines):
                value = lines[idx + 2].strip()
                if value:
                    return value
        return None

    # --------------------------------------------------------------- snapshots
    def get_snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {
                "bearer_token": self._bearer_token,
                "entity_list": list(self._entity_payloads),
                "entity_lines": "\n".join(self._entity_payloads),
                "entity_count": len(self._entity_payloads),
                "api_endpoint": self._api_endpoint,
            }

    def fetch_token_from_storage(self) -> Optional[str]:
        if not self.driver:
            return None
        try:
            token = self.driver.execute_script(
                "return localStorage.getItem('@secure.s.token') || "
                "sessionStorage.getItem('@secure.s.token');"
            )
        except WebDriverException:
            return None
        if token:
            self._store_bearer_token(token)
        return token

    def wait_for_token(self, timeout: Optional[float] = None) -> bool:
        return self._token_event.wait(timeout)

    def wait_for_entities(self, min_count: int = 1, timeout: Optional[float] = None) -> bool:
        if min_count <= 0:
            return True
        end_time = None if timeout is None else time.time() + timeout
        while True:
            with self._lock:
                if len(self._entity_payloads) >= min_count:
                    return True
            remaining = None if end_time is None else end_time - time.time()
            if remaining is not None and remaining <= 0:
                return False
            self._entity_event.wait(timeout=0.5)

    def save_snapshot(self, output_file: str = "captured_credentials.json"):
        snapshot = self.get_snapshot()
        payload = {
            "bearer_token": snapshot["bearer_token"],
            "entity_lines": snapshot["entity_lines"],
            "entity_lines_list": snapshot["entity_list"],
            "api_endpoint": snapshot["api_endpoint"],
        }
        out_path = Path(output_file)
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return out_path

    # -------------------------------------------------------------------- stop
    def stop(self, close_browser: bool = True):
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)
            self._monitor_thread = None
        if close_browser and self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
        self.driver = None
