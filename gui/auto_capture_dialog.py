"""Qt dialog that runs AutoCaptureSession in-process for seamless UX."""

from __future__ import annotations

import threading
import time
from typing import Optional

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QLabel,
    QTextEdit,
    QHBoxLayout,
    QMessageBox,
)

from auto_capture_session import AutoCaptureSession
from .widgets import ModernButton


class AutoCaptureWorker(QThread):
    status_update = Signal(str)
    snapshot = Signal(dict)
    error = Signal(str)

    def __init__(self):
        super().__init__()
        self._stop_event = threading.Event()
        self._close_browser = True
        self.session: Optional[AutoCaptureSession] = None

    def run(self):
        try:
            self.session = AutoCaptureSession()
            self.session.start()
            self.session.open_portal()
            self.session.start_monitoring()
            self.status_update.emit(
                "Chrome dibuka. Login ke SIKS lalu buka daftar keluarga untuk mulai capture."
            )

            while not self._stop_event.is_set():
                snapshot = self.session.get_snapshot()
                self.snapshot.emit(snapshot)
                time.sleep(1)

        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            if self.session:
                try:
                    self.session.stop(close_browser=self._close_browser)
                except Exception:
                    pass

    def request_stop(self, close_browser: bool = True):
        self._close_browser = close_browser
        self._stop_event.set()

    def latest_snapshot(self) -> Optional[dict]:
        if self.session:
            return self.session.get_snapshot()
        return None


class AutoCaptureDialog(QDialog):
    """Dialog that manages the auto capture worker and streams updates."""

    credentials_ready = Signal(str, str, int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🤖 Auto Capture - SIKS Data")
        self.setModal(True)
        self.resize(580, 420)

        self.worker: Optional[AutoCaptureWorker] = None
        self.latest_snapshot: Optional[dict] = None
        self._last_notified_hash: Optional[str] = None

        self._build_ui()

    # ------------------------------------------------------------------ UI
    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 25, 30, 25)
        layout.setSpacing(18)

        title = QLabel("🤖 Automatic SIKS Data Capture")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet(
            "font-size: 18px; font-weight: bold; color: #2F75B5; margin-bottom: 6px;"
        )
        layout.addWidget(title)

        instructions = (
            "1. Klik ‘Mulai Auto Capture’. Chrome akan terbuka tanpa terminal tambahan.\n"
            "2. Login manual dan buka daftar keluarga/pagination yang ingin diambil.\n"
            "3. Data token & entity akan langsung dikirim ke form Konfigurasi saat tertangkap.\n"
            "4. Tutup dialog kapan saja – hasil terbaru otomatis divalidasi."
        )

        self.instructions_box = QTextEdit()
        self.instructions_box.setReadOnly(True)
        self.instructions_box.setPlainText(instructions)
        self.instructions_box.setMinimumHeight(130)
        self.instructions_box.setStyleSheet(
            """
            QTextEdit {
                background: #1a1a2e;
                color: #ecf0f1;
                border: 1px solid #3a85c5;
                border-radius: 8px;
                padding: 12px;
                font-size: 12px;
            }
            QTextEdit QScrollBar:vertical { width: 10px; }
            """
        )
        layout.addWidget(self.instructions_box)

        self.status_label = QLabel("Siap menjalankan auto capture.")
        self.status_label.setAlignment(Qt.AlignCenter)
        self._set_status("info", "Siap menjalankan auto capture.")
        layout.addWidget(self.status_label)

        metrics_row = QHBoxLayout()
        metrics_row.setSpacing(20)

        self.token_label = QLabel("🔑 Token: belum")
        self.token_label.setStyleSheet("font-size: 13px; color: #bdc3c7;")
        metrics_row.addWidget(self.token_label)

        self.entity_label = QLabel("📄 Halaman tertangkap: 0")
        self.entity_label.setStyleSheet("font-size: 13px; color: #bdc3c7;")
        metrics_row.addWidget(self.entity_label)

        layout.addLayout(metrics_row)

        # Auto-close checkbox
        from PySide6.QtWidgets import QCheckBox
        self.chk_auto_close = QCheckBox("Tutup otomatis setelah capture berhasil (3s)")
        self.chk_auto_close.setChecked(True)
        self.chk_auto_close.setStyleSheet("color: #ecf0f1; font-size: 12px;")
        layout.addWidget(self.chk_auto_close)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(15)

        self.close_btn = ModernButton("Tutup", danger=True)
        self.close_btn.clicked.connect(self.reject)
        btn_row.addWidget(self.close_btn)

        self.stop_btn = ModernButton("⏹️ Stop", danger=True)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(lambda: self._stop_worker(close_browser=True))
        btn_row.addWidget(self.stop_btn)

        self.start_btn = ModernButton("▶️ Mulai Auto Capture", primary=True)
        self.start_btn.clicked.connect(self._start_worker)
        btn_row.addWidget(self.start_btn)

        layout.addLayout(btn_row)

        # Timer for auto-close
        from PySide6.QtCore import QTimer
        self.auto_close_timer = QTimer(self)
        self.auto_close_timer.setSingleShot(True)
        self.auto_close_timer.timeout.connect(self.accept)

    # ---------------------------------------------------------- UI helpers
    def _set_status(self, level: str, message: str):
        colors = {
            "info": "#3498db",
            "success": "#27ae60",
            "warning": "#f39c12",
            "error": "#e74c3c",
        }
        color = colors.get(level, "#3498db")
        self.status_label.setText(message)
        self.status_label.setStyleSheet(
            f"font-size: 14px; font-weight: bold; color: {color}; "
            "padding: 10px; background: #2c3e50; border-radius: 6px;"
        )

    # ----------------------------------------------------------- worker ctrl
    def _start_worker(self):
        if self.worker and self.worker.isRunning():
            return
        self.worker = AutoCaptureWorker()
        self.worker.status_update.connect(lambda msg: self._set_status("info", msg))
        self.worker.snapshot.connect(self._handle_snapshot)
        self.worker.error.connect(self._handle_error)
        self.worker.start()
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self._set_status("info", "Menjalankan Chrome dan memonitor jaringan...")

    def _stop_worker(self, close_browser: bool = False):
        if self.worker and self.worker.isRunning():
            self.worker.request_stop(close_browser=close_browser)
            self.worker.wait(5000)
        self.worker = None
        self.stop_btn.setEnabled(False)
        self.start_btn.setEnabled(True)
        self.auto_close_timer.stop()

    # -------------------------------------------------------------- handlers
    def _handle_snapshot(self, snapshot: dict):
        self.latest_snapshot = snapshot
        token = snapshot.get("bearer_token")
        count = snapshot.get("entity_count", 0)
        lines = snapshot.get("entity_lines", "")

        if token:
            self.token_label.setText(f"🔑 Token: {token[:24]}...")
        else:
            self.token_label.setText("🔑 Token: belum")

        self.entity_label.setText(f"📄 Halaman tertangkap: {count}")

        if token and count > 0:
            snapshot_hash = f"{token}:{count}:{len(lines)}"
            if snapshot_hash != self._last_notified_hash:
                self._last_notified_hash = snapshot_hash

                msg = f"Auto capture aktif • Token siap • {count} halaman terisi"
                if self.chk_auto_close.isChecked():
                    msg += " • Menutup dalam 3s..."
                    # Restart timer on new data
                    self.auto_close_timer.start(3000)

                self._set_status("success", msg)
                self.credentials_ready.emit(token, lines, count)

    def _handle_error(self, message: str):
        QMessageBox.critical(self, "Auto Capture Error", message)
        self._set_status("error", message)
        self._stop_worker(close_browser=False)

    # --------------------------------------------------------------- events
    def closeEvent(self, event):
        self._stop_worker()
        if self.latest_snapshot:
            token = self.latest_snapshot.get("bearer_token")
            lines = self.latest_snapshot.get("entity_lines", "")
            count = self.latest_snapshot.get("entity_count", 0)
            if token and count > 0:
                self.credentials_ready.emit(token, lines, count)
        event.accept()