"""
CLI utility that reuses AutoCaptureSession for manual workflows.
"""

from __future__ import annotations

import signal
import time
from typing import Optional

from auto_capture_session import AutoCaptureSession


def run_cli():
    session = AutoCaptureSession()
    session.start()
    session.open_portal()
    session.start_monitoring()

    print("\n🚀 Selenium Chrome launched. Please login manually inside the browser.")
    print("   • Navigate to menu DTSEN dan buka daftar keluarga.")
    print("   • Setiap kali Anda membuka halaman/list baru, data otomatis ditangkap.")
    print("   • Tekan CTRL+C di terminal ini kapan saja untuk menyimpan hasil tanpa menutup Chrome terlebih dahulu.\n")

    last_count = 0
    token_notified = False

    def handle_interrupt(signum, frame):  # type: ignore[override]
        raise KeyboardInterrupt

    old_handler = signal.signal(signal.SIGINT, handle_interrupt)

    try:
        while True:
            snapshot = session.get_snapshot()
            if snapshot["bearer_token"] and not token_notified:
                preview = snapshot["bearer_token"][:32]
                print(f"🔑 Bearer token tertangkap: {preview}...")
                token_notified = True

            if snapshot["entity_count"] > last_count:
                last_count = snapshot["entity_count"]
                print(f"📄 Halaman tertangkap: {last_count}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n⏹️ Menghentikan capture dan menyimpan hasil...")
    finally:
        signal.signal(signal.SIGINT, old_handler)
        if not token_notified:
            session.fetch_token_from_storage()
        output_path = session.save_snapshot()
        snapshot = session.get_snapshot()
        session.stop(close_browser=True)

        print("\n" + "=" * 60)
        print("📋 CAPTURE SUMMARY")
        print("=" * 60)
        print(f"Bearer Token: {'✅' if snapshot['bearer_token'] else '❌'}")
        print(f"Entity Lines: {snapshot['entity_count']} halaman")
        print(f"File: {output_path.resolve()}")
        print("Chrome browser telah ditutup. Anda dapat menjalankan ulang jika diperlukan.")


def main():
    try:
        run_cli()
    except Exception as exc:
        print(f"\n❌ Error: {exc}")


if __name__ == "__main__":
    main()
