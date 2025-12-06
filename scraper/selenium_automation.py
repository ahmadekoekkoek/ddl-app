"""
SIKS Auto-Capture using Selenium (REAL Chrome)
Perfect rendering + Real-time entity capture
"""

import os
import json
import time
import threading
from pathlib import Path
from typing import Optional, Dict
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

class SIKSSeleniumAutomator:
    """Automates SIKS using Selenium with REAL Chrome browser."""

    def __init__(self, siks_url: str = "https://siks.kemensos.go.id"):
        self.siks_url = siks_url
        self.driver = None
        self.captured_data = {
            "bearer_token": None,
            "entity_lines": ""
        }
        self.entity_payloads = []
        self._monitoring = False
        self._login_complete = False
        self._data_captured = False

    def start(self):
        """Initialize Chrome browser with optimal settings."""
        print("\n🚀 Starting SIKS Auto-Capture with Selenium...")
        print("=" * 60)

        # Chrome options for best experience
        options = Options()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Enable performance logging to intercept network requests
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

        # Initialize driver
        print("📦 Setting up ChromeDriver...")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # Anti-detection
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            '''
        })

        print("✅ Chrome browser started!")
        print("   This is REAL Chrome - rendering will be perfect!")

    def navigate_and_wait_for_login(self):
        """Navigate to SIKS and wait for manual login."""
        print(f"\n🌐 Opening {self.siks_url}...")
        self.driver.get(self.siks_url)

        # Start background log monitoring
        print("   📡 Starting background network monitoring...")
        self._start_log_monitoring()

        print("\n" + "=" * 60)
        print("🔐 PLEASE LOGIN MANUALLY")
        print("=" * 60)
        print("1. Login with your SIKS credentials in the Chrome window")
        print("2. Navigate around the site after login")
        print("3. **IMPORTANT**: Click on family/entity list to trigger API calls")
        print("4. You'll see entity captures appear below as you click")
        print("5. Press Enter when done...")
        print()
        print("💡 TIP: Entity payloads will be captured automatically as you browse!")
        print()

        input("Press Enter after you're done: ")

        # Stop monitoring
        self._stop_log_monitoring()

    def _start_log_monitoring(self):
        """Start continuous performance log monitoring in background."""
        self._monitoring = True
        self._processed_requests = set()

        def monitor_logs():
            while self._monitoring:
                try:
                    logs = self.driver.get_log('performance')

                    for entry in logs:
                        try:
                            log = json.loads(entry['message'])['message']

                            if log['method'] == 'Network.requestWillBeSent':
                                request = log['params']['request']
                                url = request.get('url', '')
                                request_id = log['params'].get('requestId', '')

                                # Capture entity API calls
                                if 'get-keluarga-dtsen' in url and request.get('method') == 'POST':
                                    if request_id in self._processed_requests:
                                        continue

                                    self._processed_requests.add(request_id)
                                    post_data = request.get('postData', '')

                                    if 'entity' in post_data:
                                        lines = post_data.split('\r\n')
                                        for i, line in enumerate(lines):
                                            if 'name="entity"' in line:
                                                if i + 2 < len(lines):
                                                    entity = lines[i + 2].strip()
                                                    if len(entity) > 20 and entity not in self.entity_payloads:
                                                        self.entity_payloads.append(entity)
                                                        print(f"\n   🎯 Captured entity #{len(self.entity_payloads)}: {entity[:40]}...")
                                                        break

                                # Capture bearer token
                                if not self.captured_data.get("bearer_token"):
                                    headers = request.get('headers', {})
                                    auth_header = headers.get('authorization') or headers.get('Authorization')

                                    if auth_header and len(auth_header) > 10:
                                        token = auth_header.replace('Bearer ', '').replace('bearer ', '').strip()
                                        self.captured_data["bearer_token"] = token
                                        print(f"\n   🔑 Token captured: {token[:20]}...")

                        except:
                            continue

                    time.sleep(0.5)

                except:
                    time.sleep(0.5)

        self._monitor_thread = threading.Thread(target=monitor_logs, daemon=True)
        self._monitor_thread.start()

    def _stop_log_monitoring(self):
        """Stop log monitoring."""
        self._monitoring = False
        if hasattr(self, '_monitor_thread'):
            self._monitor_thread.join(timeout=1)

    def capture_bearer_token(self) -> Optional[str]:
        """Return the bearer token captured by background monitoring."""
        print("\n🔑 Finalizing bearer token capture...")

        bearer_token = self.captured_data.get("bearer_token")

        if bearer_token:
            print(f"   ✅ Bearer token ready: {bearer_token[:20]}...")
            return bearer_token
        else:
            print("   ⚠️  No bearer token was captured during browsing")
            return None

    def capture_entity_lines(self) -> Optional[str]:
        """Finalize entity lines from background-collected payloads."""
        print("\n📋 Finalizing entity lines...")

        if self.entity_payloads:
            entity_lines = "\n".join(self.entity_payloads)
            self.captured_data["entity_lines"] = entity_lines
            print(f"   ✅ Total entities collected: {len(self.entity_payloads)}")
            return entity_lines
        else:
            print("   ⚠️  No entity payloads were captured")
            return None

    def save_results(self, output_file: str = "captured_credentials.json"):
        """Save captured data to file."""
        output_path = Path(output_file)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.captured_data, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Results saved to: {output_path.absolute()}")

    def cleanup(self):
        """Close browser."""
        if self.driver:
            print("\n🔒 Closing browser...")
            self.driver.quit()

    def run(self) -> Dict:
        """Execute full automation flow."""
        try:
            self.start()
            self.navigate_and_wait_for_login()

            bearer_token = self.capture_bearer_token()
            entity_lines = self.capture_entity_lines()

            self.save_results()

            # Summary
            print("\n" + "=" * 60)
            print("📋 CAPTURE SUMMARY")
            print("=" * 60)
            print(f"Bearer Token: {'✅ Captured' if bearer_token else '❌ Failed'}")
            print(f"Entity Lines: {'✅ Captured (' + str(len(self.entity_payloads)) + ' entities)' if entity_lines else '❌ Failed'}")

            if bearer_token and entity_lines:
                print("\n🎉 SUCCESS! All credentials captured in correct format.")
                print(f"\n  bearer_token: {bearer_token[:30]}...")
                print(f"  entity_lines: {len(self.entity_payloads)} entity payload(s)")
            else:
                print("\n⚠️  Partial capture - please review results")

            return self.captured_data

        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
            return self.captured_data

        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return self.captured_data

        finally:
            print("\n\nPress Enter to close browser...")
            input()
            self.cleanup()

    def wait_for_login(self):
        """Wait for user to complete login manually"""
        print("\n🔐 Please log into SIKS website manually...")
        print("💡 Once logged in, navigate to the data page to capture entities")

        # Navigate to SIKS
        self.driver.get(self.siks_url)

        # Wait for login completion (check for dashboard or typical post-login elements)
        while not self._login_complete:
            try:
                # Check if we're past login (look for common post-login indicators)
                current_url = self.driver.current_url

                # Common indicators that login is complete
                if any(indicator in current_url.lower() for indicator in [
                    'dashboard', 'main', 'home', 'index', 'menu', 'dtks'
                ]):
                    self._login_complete = True
                    print("✅ Login detected! Starting data monitoring...")
                    break

                # Also check for logout button or user info elements
                try:
                    from selenium.webdriver.common.by import By
                    logout_elements = self.driver.find_elements(By.CSS_SELECTOR,
                        "button[onclick*='logout'], a[href*='logout'], .logout, .user-menu")
                    if logout_elements:
                        self._login_complete = True
                        print("✅ Login detected via logout element! Starting data monitoring...")
                        break
                except:
                    pass

                time.sleep(2)  # Check every 2 seconds

            except Exception as e:
                print(f"⚠️ Waiting for login... {str(e)[:50]}")
                time.sleep(2)

    def start_monitoring(self):
        """Start monitoring for authorization tokens and entity data"""
        if not self._login_complete:
            print("❌ Login not complete. Call wait_for_login() first.")
            return

        self._monitoring = True
        print("\n🔍 Monitoring network requests for tokens and entity data...")

        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_network)
        monitor_thread.daemon = True
        monitor_thread.start()

        print("💡 Please navigate to the entity/family data page in SIKS")
        print("   The system will automatically capture authorization and entity data")


def main():
    print("=" * 60)
    print("🤖 SIKS AUTO-CAPTURE (Selenium + Real Chrome)")
    print("=" * 60)
    print("\n✨ Features:")
    print("  • Uses your system's Chrome browser")
    print("  • Perfect rendering (no visual glitches)")
    print("  • Real-time entity capture as you browse")
    print("  • Auto-captures bearer_token + entity_lines")

    print("\n📝 Press Enter to start, or Ctrl+C to cancel...")
    input()

    automator = SIKSSeleniumAutomator()
    automator.run()

    print("\n✅ Done! Check captured_credentials.json")


if __name__ == "__main__":
    main()
