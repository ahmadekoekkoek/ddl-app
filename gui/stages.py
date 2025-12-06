"""
GUI Stages Module
All stage widget classes for the application workflow
"""

import os
import json
import time
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit,
    QTextEdit, QPushButton, QFrame, QFileDialog, QSizePolicy, QMessageBox,
    QGraphicsDropShadowEffect, QScrollArea
)
from PySide6.QtCore import Qt, Signal, QUrl, QTimer
from PySide6.QtGui import QColor, QDesktopServices
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineSettings

from .widgets import CircularProgress, ModernButton, PackageCard
from .workers import ConfigValidationWorker
from .flow_layout import FlowLayout
from .errors import show_error_dialog, ConfigError
from .auto_capture_dialog import AutoCaptureDialog
from system_info import get_system_specs, estimate_speed, get_default_output_folder, measure_network_metrics


class StageWidget(QWidget):
    """Base class for each stage"""
    def __init__(self, title):
        super().__init__()
        self.title = title
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 10, 20, 20)  # Reduced margins for better small-window layout
        # Title removed - shown in step indicator instead

    def on_enter(self):
        """Called when stage becomes active"""
        pass


class ConfigStage(StageWidget):
    """Stage for manual config input (authorization and entity)"""
    capture_complete = Signal(str, list, int)  # authorization, entities, total_families
    families_extracted = Signal(list)  # Pre-captured families for optimized scraping

    def __init__(self):
        self.validation_worker = None
        self._pending_auto_validation = False
        self._auto_capture_hash = None
        self._auto_capture_count = 0
        self._auto_navigate_after_validation = False
        self._navigate_callback = None
        self.pre_captured_families = []  # Store families for optimized scraping
        super().__init__("⚙️ Konfigurasi")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Auto Capture Section (Prominent)
        self.auto_capture_frame = QFrame()
        self.auto_capture_frame.setStyleSheet("""
            QFrame {
                background: #252538;
                border-radius: 10px;
                padding: 15px;
                margin-bottom: 10px;
            }
        """)
        auto_layout = QVBoxLayout(self.auto_capture_frame)

        auto_title = QLabel("🤖 Mulai Scrape Otomatis")
        auto_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #9b59b6; margin-bottom: 5px;")
        auto_title.setAlignment(Qt.AlignCenter)
        auto_layout.addWidget(auto_title)

        auto_desc = QLabel("Metode disarankan - otomatis menangkap token dari browser SIKS-NG")
        auto_desc.setStyleSheet("font-size: 12px; color: #bdc3c7; margin-bottom: 8px;")
        auto_desc.setAlignment(Qt.AlignCenter)
        auto_desc.setWordWrap(True)
        auto_layout.addWidget(auto_desc)

        self.btn_auto_capture = ModernButton("🚀 Jalankan Auto Capture")
        self.btn_auto_capture.setMinimumHeight(45)
        self.btn_auto_capture.clicked.connect(self.open_auto_capture_dialog)
        self.btn_auto_capture.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #9b59b6, stop: 1 #8e44ad);
                color: white;
                border: none;
                border-radius: 8px;
                font-weight: bold;
                font-size: 16px;
            }
            QPushButton:hover {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #a569bd, stop: 1 #9c56c2);
            }
            QPushButton:pressed {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #8e44ad, stop: 1 #7d3c98);
            }
        """)
        auto_layout.addWidget(self.btn_auto_capture)

        layout.addWidget(self.auto_capture_frame)

        # Manual Input Toggle
        self.manual_toggle_btn = QPushButton("🛠️ Tampilkan Input Manual")
        self.manual_toggle_btn.setCheckable(True)
        self.manual_toggle_btn.setStyleSheet("""
            QPushButton {
                background: transparent;
                color: #3498db;
                border: 1px solid #3498db;
                border-radius: 4px;
                padding: 8px;
                font-size: 12px;
            }
            QPushButton:checked {
                background: #3498db;
                color: white;
            }
            QPushButton:hover {
                background: rgba(52, 152, 219, 0.1);
            }
        """)
        self.manual_toggle_btn.toggled.connect(self.toggle_manual_input)
        layout.addWidget(self.manual_toggle_btn)

        # Manual Input Container (Hidden by default)
        self.manual_container = QWidget()
        self.manual_container.setVisible(False)
        manual_layout = QVBoxLayout(self.manual_container)
        manual_layout.setContentsMargins(0, 10, 0, 0)

        # Authorization input section
        auth_label = QLabel("Authorisasi:")
        auth_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #ecf0f1;")
        manual_layout.addWidget(auth_label)

        self.auth_input = QLineEdit()
        self.auth_input.setPlaceholderText("Paste kode authorisasi dari browser DevTools...")
        self.auth_input.setStyleSheet("""
            QLineEdit {
                padding: 12px;
                border: 2px solid #3a85c5;
                border-radius: 6px;
                font-size: 13px;
                background: #2a2a3e;
                color: white;
                font-family: 'Consolas', monospace;
            }
            QLineEdit:focus {
                border: 2px solid #2ecc71;
            }
        """)
        manual_layout.addWidget(self.auth_input)

        manual_layout.addSpacing(15)

        # Entity input section
        entity_label = QLabel("Data Entitas:")
        entity_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #ecf0f1;")
        manual_layout.addWidget(entity_label)

        self.entity_input = QTextEdit()
        self.entity_input.setPlaceholderText("Paste entity lines (satu per baris)...")
        self.entity_input.setMinimumHeight(80)
        self.entity_input.setStyleSheet("""
            QTextEdit {
                padding: 12px;
                border: 2px solid #3a85c5;
                border-radius: 6px;
                font-size: 13px;
                background: #2a2a3e;
                color: white;
                font-family: 'Consolas', monospace;
            }
            QTextEdit:focus {
                border: 2px solid #2ecc71;
            }
        """)
        manual_layout.addWidget(self.entity_input)

        # Manual Help Button
        btn_manual = ModernButton("📖 PANDUAN MANUAL CAPTURE")
        btn_manual.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("file:///c:/Users/USER/prog_premium_2.0/MANUAL_CAPTURE_GUIDE.md")))
        manual_layout.addWidget(btn_manual)

        layout.addWidget(self.manual_container)

        layout.addSpacing(10)

        # Status label
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("font-size: 14px; color: #f39c12; font-weight: bold;")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setMinimumHeight(30)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(15)

        # Reset button
        self.btn_load = ModernButton("🔄 Reset", danger=True)
        self.btn_load.setMinimumHeight(45)
        self.btn_load.clicked.connect(self.reset_fields)
        self.btn_load.setShortcut("Ctrl+R")
        self.btn_load.setToolTip("Reset Fields (Ctrl+R)")
        btn_layout.addWidget(self.btn_load)

        # Save & validate button
        self.btn_save = ModernButton("💾 Simpan & Validasi", primary=True)
        self.btn_save.setMinimumHeight(45)
        self.btn_save.clicked.connect(self.save_and_validate)
        self.btn_save.setShortcut("Ctrl+S")
        self.btn_save.setToolTip("Simpan & Validasi (Ctrl+S)")
        btn_layout.addWidget(self.btn_save)

        layout.addLayout(btn_layout)
        layout.addWidget(self.status_label)

        layout.addStretch()

        # Try to load existing config on startup
        self._try_load_existing_config()

    def toggle_manual_input(self, checked):
        self.manual_container.setVisible(checked)
        self.auto_capture_frame.setVisible(not checked)  # Hide auto-capture section when manual mode is on
        if checked:
            self.manual_toggle_btn.setText("🛠️ Sembunyikan Input Manual")
        else:
            self.manual_toggle_btn.setText("🛠️ Tampilkan Input Manual")

    def reset_fields(self):
        """Reset/clear all input fields"""
        self.auth_input.clear()
        self.entity_input.clear()
        self.status_label.setText("")
        self._auto_capture_hash = None
        self._auto_capture_count = 0
        self._pending_auto_validation = False

    def open_auto_capture_dialog(self):
        """Open the auto capture automation dialog"""
        dialog = AutoCaptureDialog(self)
        dialog.credentials_ready.connect(self.apply_auto_capture)
        dialog.exec()

    def apply_auto_capture(self, token: str, entity_lines: str, page_count: int):
        token = (token or "").strip()
        entity_lines = (entity_lines or "").strip()
        if not token or not entity_lines:
            return

        new_hash = f"{token}:{hash(entity_lines)}"
        if new_hash == self._auto_capture_hash:
            return

        self._auto_capture_hash = new_hash
        self._auto_capture_count = page_count
        self.auth_input.setText(token)
        self.entity_input.setPlainText(entity_lines)
        self.status_label.setText(
            f"🤖 Auto capture aktif • {page_count} halaman tertangkap. Memvalidasi otomatis..."
        )
        self.status_label.setStyleSheet("font-size: 14px; color: #27ae60; font-weight: bold;")

        # Prepare auto navigation callback (same behavior as manual flow)
        if not self._auto_navigate_after_validation:
            self._auto_navigate_after_validation = True
            self._navigate_callback = lambda: self.capture_complete.emit(
                getattr(self, "last_validated_auth", token),
                getattr(self, "last_validated_entities", []),
                getattr(self, "last_validated_total", 0),
            )

        self._trigger_validation_after_auto_capture()

    def _trigger_validation_after_auto_capture(self):
        if self.btn_save.isEnabled():
            self._pending_auto_validation = False
            self.save_and_validate()
        else:
            self._pending_auto_validation = True

    def _try_load_existing_config(self):
        """Try to load existing config.json on startup"""
        try:
            if os.path.exists("config.json"):
                with open("config.json", "r") as f:
                    config = json.load(f)
                    if "bearer_token" in config:
                        self.auth_input.setText(config["bearer_token"])
                    if "entity_lines" in config:
                        self.entity_input.setText(config["entity_lines"])
                self.status_label.setText("✅ Config.json ditemukan dan dimuat")
                self.status_label.setStyleSheet("font-size: 14px; color: #27ae60; font-weight: bold;")
        except Exception as e:
            print(f"[ConfigStage] Error loading config: {e}")
            self.status_label.setText(f"❌ Error memuat config.json: {str(e)}")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")

    def load_from_config(self):
        """Load authorization and entities from config.json"""
        try:
            if os.path.exists("config.json"):
                with open("config.json", "r") as f:
                    config = json.load(f)

                self.auth_input.setText(config.get("bearer_token", ""))
                self.entity_input.setPlainText(config.get("entity_lines", ""))
                self.status_label.setText("✅ Data dimuat dari config.json")
                self.status_label.setStyleSheet("font-size: 14px; color: #27ae60; font-weight: bold;")
            else:
                self.status_label.setText("❌ config.json tidak ditemukan!")
                self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")
        except Exception as e:
            self.status_label.setText(f"❌ Error: {str(e)[:50]}")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")

    def save_and_validate(self):
        """Save to config.json and validate"""
        self._pending_auto_validation = False
        authorization = self.auth_input.text().strip()
        entity_text = self.entity_input.toPlainText().strip()
        entities = [e.strip() for e in entity_text.split("\n") if e.strip()]

        if not authorization:
            self.status_label.setText("❌ Authorization token kosong!")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")
            show_error_dialog(self, ConfigError("Authorization token tidak boleh kosong"), "Input Error")
            return

        if not entities:
            self.status_label.setText("❌ Entity payload kosong!")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")
            show_error_dialog(self, ConfigError("Entity payload tidak boleh kosong"), "Input Error")
            return

        # Get AES key from env
        aes_key = os.getenv("AES_BASE64_KEY")
        if not aes_key:
            self.status_label.setText("❌ AES_BASE64_KEY tidak ditemukan di .env!")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")
            show_error_dialog(self, ConfigError("AES_BASE64_KEY tidak ditemukan di .env"), "Config Error")
            return

        # Disable button
        self.btn_save.setEnabled(False)
        self.btn_save.setText("⏳ Memvalidasi...")

        # Start validation worker
        self.validation_worker = ConfigValidationWorker(authorization, entities, aes_key)
        self.validation_worker.status_update.connect(self.update_status)
        self.validation_worker.error.connect(self.on_validation_error)
        self.validation_worker.validated.connect(self.on_validation_success)
        self.validation_worker.families_extracted.connect(self.on_families_extracted)
        self.validation_worker.start()

    def update_status(self, message):
        self.status_label.setText(message)
        self.status_label.setStyleSheet("font-size: 14px; color: #f39c12; font-weight: bold;")

    def on_validation_error(self, message):
        self.status_label.setText(f"❌ {message}")
        self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c; font-weight: bold;")

        # Show dialog
        show_error_dialog(self, message, "Validasi Gagal")

        self.btn_save.setEnabled(True)
        self.btn_save.setText("💾 Simpan & Validasi")
        if self._pending_auto_validation:
            self._pending_auto_validation = False
            self.save_and_validate()

    def on_families_extracted(self, families):
        """Store pre-captured families for later scraping optimization"""
        self.pre_captured_families = families
        self.families_extracted.emit(families)
        print(f"[ConfigStage] Stored {len(families)} pre-captured families for optimized scraping")

    def on_validation_success(self, authorization, entities, total_families):
        # Show results but STAY on this page (per requirement):
        if self._auto_capture_count:
            self.status_label.setText(
                f"✅ Auto capture valid! {self._auto_capture_count} halaman • {total_families} keluarga unik."
            )
        else:
            self.status_label.setText(
                f"✅ Valid! {total_families} keluarga unik ditemukan. Tekan 'Lanjutkan'."
            )
        self.status_label.setStyleSheet("font-size: 14px; color: #27ae60; font-weight: bold;")
        # Re-enable button so user can validate again if needed
        self.btn_save.setEnabled(True)
        self.btn_save.setText("💾 Simpan & Validasi")

        # Save to config
        self._save_to_config(authorization, entities)

        # Store last validated data so 'Lanjutkan' can use it to show counts
        self.last_validated_auth = authorization
        self.last_validated_entities = list(entities)
        self.last_validated_total = int(total_families)

        # If auto_navigate flag is set, automatically proceed to confirm stage
        if hasattr(self, '_auto_navigate_after_validation') and self._auto_navigate_after_validation:
            self._auto_navigate_after_validation = False  # Reset flag
            # Emit signal to trigger navigation
            if hasattr(self, '_navigate_callback') and callable(self._navigate_callback):
                self._navigate_callback()
        if self._pending_auto_validation:
            self._pending_auto_validation = False
            self.save_and_validate()

    def _save_to_config(self, authorization: str, entities: list):
        """Save data to config.json"""
        config = {
            "bearer_token": authorization,
            "entity_lines": "\n".join(e.strip() for e in entities if e.strip()),
        }
        try:
            with open("config.json", "w", encoding="utf-8") as fh:
                json.dump(config, fh, indent=2)
            print(f"[ConfigStage] Saved to config.json: auth={authorization[:30]}..., entities={len(entities)}")
        except Exception as e:
            print(f"[ConfigStage] Error saving config.json: {e}")




class PreScrapeConfirmStage(StageWidget):
    def __init__(self):
        self.selected_folder = None
        super().__init__("📋 Konfirmasi Data")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        self.info_label = QLabel()
        self.info_label.setStyleSheet("""
            font-size: 16px;
            color: #ecf0f1;
            margin: 20px;
            padding: 20px;
            background: #2c3e50;
            border-radius: 10px;
        """)
        self.info_label.setAlignment(Qt.AlignCenter)
        self.info_label.setWordWrap(True)
        layout.addWidget(self.info_label)

        cards_container = QWidget()
        cards_layout = QHBoxLayout(cards_container)
        cards_layout.setContentsMargins(20, 10, 20, 10)
        cards_layout.setSpacing(20)
        layout.addWidget(cards_container)

        folder_frame = QFrame()
        folder_frame.setStyleSheet("""
            QFrame {
                background: #252538;
                border-radius: 8px;
                padding: 18px;
            }
        """)
        folder_layout = QVBoxLayout(folder_frame)
        folder_layout.setSpacing(12)

        folder_title = QLabel("📁 Lokasi Output")
        folder_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #3498db;")
        folder_layout.addWidget(folder_title)

        self.folder_status_label = QLabel()
        self.folder_status_label.setStyleSheet("font-size: 12px; color: #ecf0f1;")
        folder_layout.addWidget(self.folder_status_label)

        self.folder_label = QLabel()
        self.folder_label.setStyleSheet("""
            font-size: 12px;
            color: #95a5a6;
            padding: 10px;
            background: #1a1a2e;
            border-radius: 6px;
            border: 1px solid #3a3a5e;
        """)
        self.folder_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.folder_label.setMinimumHeight(52)
        self.folder_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.folder_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.folder_label.setWordWrap(True)
        folder_layout.addWidget(self.folder_label)

        self.folder_hint_label = QLabel("File akan otomatis dibuat di dalam folder TX_xxx.")
        self.folder_hint_label.setStyleSheet("font-size: 11px; color: #7f8c8d;")
        self.folder_hint_label.setWordWrap(True)
        folder_layout.addWidget(self.folder_hint_label)

        self.folder_btn = ModernButton("📁 Pilih Folder Output")
        self.folder_btn.setMinimumHeight(46)
        self.folder_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.folder_btn.clicked.connect(self.select_output_folder)
        folder_layout.addWidget(self.folder_btn)

        cards_layout.addWidget(folder_frame, 1)

        sys_frame = QFrame()
        sys_frame.setStyleSheet("""
            QFrame {
                background: #252538;
                border-radius: 8px;
                padding: 18px;
            }
        """)
        sys_frame.setMinimumHeight(280)
        sys_layout = QVBoxLayout(sys_frame)
        sys_layout.setSpacing(12)

        sys_title = QLabel("⚙️ Informasi Sistem")
        sys_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #3498db;")
        sys_layout.addWidget(sys_title)

        stats_grid = QGridLayout()
        stats_grid.setContentsMargins(0, 0, 0, 0)
        stats_grid.setHorizontalSpacing(12)
        stats_grid.setVerticalSpacing(8)
        stats_grid.setColumnStretch(1, 1)

        self.cpu_value_label = QLabel("-")
        self.core_value_label = QLabel("-")
        self.ram_value_label = QLabel("-")
        self.disk_value_label = QLabel("-")
        self.network_value_label = QLabel("-")
        self.ping_value_label = QLabel("-")
        value_labels = [
            ("CPU", self.cpu_value_label),
            ("Cores / Threads", self.core_value_label),
            ("RAM", self.ram_value_label),
            ("Free Disk", self.disk_value_label),
            ("Network", self.network_value_label),
            ("Ping", self.ping_value_label),
        ]
        for row, (title, widget) in enumerate(value_labels):
            title_label = QLabel(title)
            title_label.setStyleSheet("font-size: 11px; color: #95a5a6;")
            stats_grid.addWidget(title_label, row, 0)
            widget.setStyleSheet("font-size: 12px; color: #ecf0f1; font-weight: bold;")
            widget.setWordWrap(True)
            stats_grid.addWidget(widget, row, 1)
        sys_layout.addLayout(stats_grid)

        self.speed_estimate_label = QLabel()
        self.speed_estimate_label.setStyleSheet("""
            font-size: 13px;
            color: #2ecc71;
            font-weight: bold;
        """)
        self.speed_estimate_label.setAlignment(Qt.AlignCenter)
        self.speed_estimate_label.setWordWrap(True)
        self.speed_estimate_label.setMinimumHeight(56)
        self.speed_estimate_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        sys_layout.addWidget(self.speed_estimate_label)

        cards_layout.addWidget(sys_frame, 1)

        question_label = QLabel("Apakah Anda yakin ingin melanjutkan proses scraping?")
        question_label.setStyleSheet("font-size: 14px; color: #bdc3c7; margin-top: 20px;")
        question_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(question_label)

        layout.addStretch()

        # Load system info
        self.load_system_info()
        # Set default folder
        self.update_folder_display()

    def load_system_info(self):
        """Load and display system information"""
        try:
            specs = get_system_specs()
            self.cpu_value_label.setText(specs['cpu_name'])
            core_detail = f"{specs['cpu_cores']} cores / {specs['cpu_threads']} threads"
            if specs.get('cpu_clock_ghz'):
                core_detail += f" @ {specs['cpu_clock_ghz']} GHz"
            self.core_value_label.setText(core_detail)
            self.ram_value_label.setText(
                f"{specs['ram_available_gb']} GB tersedia dari {specs['ram_total_gb']} GB"
            )
            self.disk_value_label.setText(f"{specs['disk_free_gb']} GB tersedia")
            self.network_value_label.setText("Mengukur kecepatan jaringan...")
            self.ping_value_label.setText("Mengukur ping...")
            net_metrics = measure_network_metrics()
            download_speed = net_metrics.get("download_mbps")
            ping_ms = net_metrics.get("ping_ms")
            if download_speed is not None:
                self.network_value_label.setText(f"{download_speed} Mbps (download)")
            else:
                self.network_value_label.setText("Tidak dapat mengukur jaringan")
            if ping_ms is not None:
                self.ping_value_label.setText(f"{ping_ms} ms")
            else:
                self.ping_value_label.setText("Tidak dapat mengukur ping")

            estimated_speed = estimate_speed(
                specs['cpu_cores'],
                specs['ram_total_gb'],
                download_speed,
                ping_ms
            )
            detail_bits = []
            if download_speed is not None:
                detail_bits.append(f"Net: {download_speed} Mbps")
            if ping_ms is not None:
                detail_bits.append(f"Ping: {ping_ms} ms")
            speed_suffix = f" ({' | '.join(detail_bits)})" if detail_bits else ""
            self.speed_estimate_label.setText(
                f"⚡ Estimasi Kecepatan: ~{estimated_speed} Keluarga/detik{speed_suffix}"
            )
        except Exception as e:
            self.cpu_value_label.setText("-")
            self.core_value_label.setText("-")
            self.ram_value_label.setText("-")
            self.disk_value_label.setText("-")
            self.network_value_label.setText("-")
            self.ping_value_label.setText("-")
            self.speed_estimate_label.setText("⚡ Estimasi Kecepatan: ~2-4 Keluarga/detik")

    def select_output_folder(self):
        """Open folder selection dialog"""
        folder = QFileDialog.getExistingDirectory(
            self,
            "Pilih Folder Output",
            get_default_output_folder(),
            QFileDialog.ShowDirsOnly
        )
        if folder:
            self.selected_folder = folder
            self.update_folder_display()

    def update_folder_display(self):
        """Update folder label display"""
        if self.selected_folder:
            self.folder_status_label.setText("Menggunakan folder pilihan Anda")
            self.folder_label.setText(f"{self.selected_folder}\\TX_xxx")
        else:
            default = get_default_output_folder()
            self.folder_status_label.setText("Menggunakan folder default (Desktop/DTSEN_Output)")
            self.folder_label.setText(f"{default}\\TX_xxx")

    def get_selected_folder(self):
        """Get the selected output folder"""
        return self.selected_folder

    def set_count(self, count, family_count=0):
        self.info_label.setText(
            f"Anda akan scrape "
            f"<span style='font-size: 32px; font-weight: bold; color: #3498db;'>{count}</span> "
            f"baris entitas yang berisi "
            f"<span style='font-size: 32px; font-weight: bold; color: #2ecc71;'>{family_count}</span> "
            f"keluarga."
        )


class ProcessingStage(StageWidget):
    def __init__(self):
        super().__init__("🔄 Memproses")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Circular progress
        self.progress = CircularProgress()
        progress_container = QWidget()
        progress_layout = QHBoxLayout(progress_container)
        progress_layout.addStretch()
        progress_layout.addWidget(self.progress)
        progress_layout.addStretch()
        layout.addWidget(progress_container)

        # Status label
        self.status_label = QLabel("Memulai...")
        self.status_label.setStyleSheet("font-size: 16px; color: #ecf0f1; margin-top: 20px;")
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)

        # Timer for elapsed time
        self.timer = QTimer()
        self.timer.timeout.connect(self._update_timer)
        self.start_time = None
        self.current_status_text = "Memulai..."

        # Metrics container
        metrics_frame = QFrame()
        metrics_frame.setStyleSheet("""
            QFrame {
                background: #252538;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 40px;
            }
        """)
        metrics_layout = QHBoxLayout(metrics_frame)

        # Speed label
        self.speed_label = QLabel("⚡ Kecepatan: -- Keluarga/detik")
        self.speed_label.setStyleSheet("""
            font-size: 14px;
            color: #2ecc71;
            font-weight: bold;
        """)
        self.speed_label.setAlignment(Qt.AlignCenter)
        metrics_layout.addWidget(self.speed_label)

        # Separator
        separator = QLabel("|")
        separator.setStyleSheet("color: #3a3a5e; font-size: 20px;")
        metrics_layout.addWidget(separator)

        # ETA label
        self.eta_label = QLabel("⏱️ ETA: Menghitung...")
        self.eta_label.setStyleSheet("""
            font-size: 14px;
            color: #3498db;
            font-weight: bold;
        """)
        self.eta_label.setAlignment(Qt.AlignCenter)
        metrics_layout.addWidget(self.eta_label)

        layout.addWidget(metrics_frame)

        # Log output
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        self.log_output.setStyleSheet("""
            QTextEdit {
                background: #1a1a2e;
                color: #ecf0f1;
                border: 1px solid #3a3a5e;
                border-radius: 6px;
                padding: 10px;
                font-family: 'Consolas', monospace;
                font-size: 12px;
            }
        """)
        layout.addWidget(self.log_output)

        layout.addStretch()

    def update_progress(self, current, total):
        self.progress.setMaximum(total)
        self.progress.setValue(current)

    def update_status(self, status):
        self.current_status_text = status
        self._update_display()

    def start_timer(self):
        self.start_time = time.time()
        self.timer.start(1000)  # 1 second interval
        self._update_display()

    def stop_timer(self):
        self.timer.stop()

    def _update_timer(self):
        self._update_display()

    def _update_display(self):
        if self.start_time:
            elapsed = int(time.time() - self.start_time)
            minutes = elapsed // 60
            seconds = elapsed % 60
            time_str = f"({minutes:02d}:{seconds:02d})"
            self.status_label.setText(f"{self.current_status_text} {time_str}")
        else:
            self.status_label.setText(self.current_status_text)

    def add_log(self, message):
        self.log_output.append(message)

    def update_metrics(self, speed: float, eta_seconds: int):
        """Update real-time scraping metrics"""
        self.speed_label.setText(f"⚡ Kecepatan: {speed:.2f} Keluarga/detik")

        # Format ETA
        if eta_seconds > 0:
            minutes = eta_seconds // 60
            seconds = eta_seconds % 60
            if minutes > 0:
                eta_text = f"{minutes} menit {seconds} detik"
            else:
                eta_text = f"{seconds} detik"
            self.eta_label.setText(f"⏱️ ETA: {eta_text}")
        else:
            self.eta_label.setText("⏱️ ETA: Selesai")

class PackageStage(StageWidget):
    package_selected = Signal(str)

    def __init__(self):
        # Initialize attributes BEFORE calling super().__init__()
        # because super calls setup_ui which needs these attributes
        self.families_count = 0
        self.members_count = 0
        self.price_per_keluarga = {
            "BASIC": 65,   # Rp 65 per Keluarga
            "PRO": 120     # Rp 120 per Keluarga
        }
        self.package_cards = {}  # Store card references for updating

        # Initial package info (will be updated with dynamic pricing)
        self.packages_info = [
            {
                "name": "BASIC",
                "summary": "Unlocked CSV",
                "price": "Rp 0",  # Will be updated dynamically
                "color": "#3498db",
                "features": [
                    "Data Keluarga Raw (CSV)",
                    "Data Anggota Raw (CSV)",
                    "Riwayat Bansos (PKH/BPNT)",
                    "Snapshot Aset & KYC"
                ]
            },
            {
                "name": "PRO",
                "summary": "Analitik Lengkap",
                "price": "Rp 0",  # Will be updated dynamically
                "color": "#9b59b6",
                "features": [
                    "Semua fitur BASIC",
                    "Merged Output (XLSX)",
                    "Visualisasi Chart",
                    "Laporan PDF Lengkap",
                    "Analisis Desil"
                ],
                "recommended": True
            }
        ]

        super().__init__("📦 Pilih Paket")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Summary
        self.summary_label = QLabel()
        self.summary_label.setStyleSheet("""
            font-size: 15px;
            color: #ecf0f1;
            background: rgba(46, 204, 113, 0.1);
            border: 1px solid #27ae60;
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 20px;
        """)
        self.summary_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.summary_label)

        # Pricing info
        self.pricing_info = QLabel()
        self.pricing_info.setStyleSheet("""
            font-size: 13px;
            color: #f39c12;
            background: rgba(243, 156, 18, 0.1);
            border: 1px solid #f39c12;
            border-radius: 6px;
            padding: 8px;
            margin-bottom: 15px;
        """)
        self.pricing_info.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.pricing_info)

        # Description
        desc = QLabel("Pilih paket yang sesuai dengan kebutuhan Anda:")
        desc.setStyleSheet("font-size: 14px; color: #bdc3c7; margin-bottom: 15px;")
        desc.setAlignment(Qt.AlignCenter)
        layout.addWidget(desc)

        # Package cards with responsive flow layout
        packages_container = QWidget()
        packages_layout = FlowLayout(packages_container, margin=20, h_spacing=20, v_spacing=20)

        # Create package cards
        for pkg in self.packages_info:
            card = PackageCard(pkg)
            card.mousePressEvent = lambda e, name=pkg["name"]: self.select_package(name)
            packages_layout.addWidget(card)
            self.package_cards[pkg["name"]] = card

        layout.addWidget(packages_container)
        layout.addStretch()

    def set_counts(self, families, members):
        self.families_count = families
        self.members_count = members
        self.summary_label.setText(f"✨ Hasil Scraping: <b>{families}</b> Keluarga & <b>{members}</b> Anggota berhasil diambil.")

        # Update pricing info
        basic_price = max(1000, families * self.price_per_keluarga["BASIC"])
        pro_price = max(1000, families * self.price_per_keluarga["PRO"])

        self.pricing_info.setText(
            f"💰 <b>Harga Fleksibel:</b> BASIC Rp {self.price_per_keluarga['BASIC']}/Keluarga | "
            f"PRO Rp {self.price_per_keluarga['PRO']}/Keluarga"
        )

        # Update package card prices
        for pkg_name, card in self.package_cards.items():
            if pkg_name == "BASIC":
                card.set_price(f"Rp {basic_price:,}")
            elif pkg_name == "PRO":
                card.set_price(f"Rp {pro_price:,}")

    def set_unlock_mode(self, is_unlock_mode, directory=None):
        """Configure package stage for unlock mode (existing locked files)"""
        self.unlock_mode = is_unlock_mode
        self.unlock_directory = directory

        if is_unlock_mode and directory:
            # Count locked files
            locked_count = 0
            try:
                for root, dirs, files in os.walk(directory):
                    locked_count += sum(1 for f in files if f.endswith('.locked'))
            except Exception:
                pass

            self.summary_label.setText(
                f"🔓 <b>Mode Unlock:</b> {locked_count} file terkunci ditemukan di folder yang dipilih."
            )
            self.summary_label.setStyleSheet("""
                font-size: 15px;
                color: #ecf0f1;
                background: rgba(52, 152, 219, 0.1);
                border: 1px solid #3498db;
                border-radius: 8px;
                padding: 10px;
                margin-bottom: 20px;
            """)

            # Estimate families from locked files (rough estimate)
            # Assume each family has ~5-8 files on average
            estimated_families = max(1, locked_count // 6)
            self.families_count = estimated_families
            self.members_count = estimated_families * 4  # Rough estimate

            # Update pricing
            basic_price = max(1000, estimated_families * self.price_per_keluarga["BASIC"])
            pro_price = max(1000, estimated_families * self.price_per_keluarga["PRO"])

            self.pricing_info.setText(
                f"💰 <b>Estimasi Harga:</b> ~{estimated_families} Keluarga | "
                f"BASIC ~Rp {basic_price:,} | PRO ~Rp {pro_price:,}"
            )

            # Update package card prices
            for pkg_name, card_data in self.package_cards.items():
                price_label = card_data['price_label']
                if pkg_name == "BASIC":
                    price_label.setText(f"~Rp {basic_price:,}")
                elif pkg_name == "PRO":
                    price_label.setText(f"~Rp {pro_price:,}")

    def select_package(self, package):
        self.package_selected.emit(package)

class TermsStage(StageWidget):
    """Terms of Service Stage (Task 5: Fixed width, scroller, and formatting)"""
    agreement_signal = Signal(bool) # True=Agree, False=Disagree

    def __init__(self):
        super().__init__("📜 Syarat & Ketentuan")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Terms Text - Fixed formatting with bold on separate lines
        terms_text = """
    <style>
        body {
            font-family: 'Segoe UI', sans-serif;
            line-height: 1.7;
            color: #ecf0f1;
        }
        h2 {
            color: #3498db;
            text-align: center;
            border-bottom: 2px solid #2c3e50;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        p, li {
            font-size: 13px;
        }
        ol {
            padding-left: 25px;
        }
        li {
            margin-bottom: 18px;
        }
        strong {
            color: #f1c40f;
            font-weight: 600;
        }
        .section-title {
            font-weight: bold;
            display: block;
            margin-bottom: 5px;
            color: #e67e22;
        }
        .footer {
            text-align: center;
            margin-top: 25px;
            padding-top: 15px;
            border-top: 1px solid #3a3a5e;
            font-size: 12px;
            color: #95a5a6;
        }
    </style>
    <h2>Ketentuan Penggunaan DTSEN Downloader</h2>
    <p>
        Selamat datang di DTSEN Downloader. Aplikasi ini dirancang sebagai alat bantu untuk mengotomatisasi proses pengunduhan dan pengolahan data dari sistem Data Terpadu Kesejahteraan Sosial (DTSEN). Dengan melanjutkan penggunaan aplikasi ini, Anda secara sadar dan tanpa paksaan menyetujui seluruh syarat dan ketentuan yang tercantum di bawah ini.
    </p>

    <ol>
        <li>
            <span class="section-title">1. Akseptasi dan Kepatuhan Hukum</span>
            Anda menyatakan bahwa Anda adalah pihak yang memiliki <strong>kewenangan dan izin yang sah</strong> untuk mengakses data DTSEN. Penggunaan aplikasi ini harus sepenuhnya mematuhi peraturan perundang-undangan yang berlaku di Indonesia, termasuk namun tidak terbatas pada Undang-Undang Informasi dan Transaksi Elektronik (UU ITE) dan Undang-Undang Perlindungan Data Pribadi (UU PDP).
        </li>
        <li>
            <span class="section-title">2. Tujuan Penggunaan</span>
            Aplikasi ini ditujukan untuk penggunaan <strong>internal dan non-komersial</strong>. Dilarang keras melakukan eksploitasi data untuk tujuan yang melanggar hukum, termasuk redistribusi, penjualan kembali, atau publikasi data tanpa izin tertulis dari pihak berwenang.
        </li>
        <li>
            <span class="section-title">3. Keamanan dan Kerahasiaan Data</span>
            Aplikasi memproses semua data—termasuk token otorisasi, payload, dan data yang diunduh—secara <strong>lokal pada perangkat Anda</strong>. Tidak ada data sensitif yang dikirim atau disimpan di server eksternal milik pengembang. Oleh karena itu, Anda bertanggung jawab penuh atas keamanan fisik dan digital perangkat yang Anda gunakan.
        </li>
        <li>
            <span class="section-title">4. Batasan Tanggung Jawab (Disclaimer of Liability)</span>
            Aplikasi ini disediakan "SEBAGAIMANA ADANYA" (AS IS). Pengembang tidak memberikan jaminan dalam bentuk apa pun, baik tersurat maupun tersirat, mengenai fungsionalitas, keandalan, atau ketersediaan layanan API dari pihak ketiga. Pengembang tidak bertanggung jawab atas:
            <ul>
                <li>Kerugian langsung, tidak langsung, atau konsekuensial yang timbul dari penggunaan atau ketidakmampuan menggunakan aplikasi.</li>
                <li>Perubahan pada sistem DTSEN yang menyebabkan aplikasi tidak berfungsi.</li>
                <li>Kebocoran data akibat kelalaian atau keamanan perangkat pengguna yang tidak memadai.</li>
            </ul>
        </li>
        <li>
            <span class="section-title">5. Transaksi dan Lisensi</span>
            Setiap transaksi yang dilakukan untuk membuka fitur premium bersifat <strong>final dan tidak dapat dikembalikan (non-refundable)</strong>. Kode pembuka (unlock code) yang diberikan bersifat unik untuk setiap sesi transaksi (TX-ID) dan hanya berlaku untuk paket yang dipilih.
        </li>
        <li>
            <span class="section-title">6. Hak Kekayaan Intelektual</span>
            Seluruh hak cipta, merek dagang, dan hak kekayaan intelektual lainnya yang terkait dengan perangkat lunak DTSEN Downloader adalah milik pengembang. Pengguna dilarang melakukan rekayasa balik (reverse engineering), dekompilasi, atau memodifikasi kode sumber aplikasi.
        </li>
    </ol>

    <div class="footer">
        Dengan menekan tombol <strong>“Setuju & Lanjutkan”</strong>, Anda mengonfirmasi bahwa Anda telah membaca, memahami, dan sepakat untuk terikat pada seluruh ketentuan ini.
    </div>
"""

        # Scroll Area for better UX
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_area.setStyleSheet("""
            QScrollArea {
                border: 2px solid #3a3a5e;
                border-radius: 10px;
                background-color: #1f1f30;
            }
            QScrollBar:vertical {
                background: #252538;
                width: 12px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical {
                background: #3498db;
                border-radius: 6px;
                min-height: 30px;
            }
            QScrollBar::handle:vertical:hover {
                background: #5dade2;
            }
        """)

        self.text_display = QTextEdit()
        self.text_display.setHtml(terms_text)
        self.text_display.setReadOnly(True)
        self.text_display.setStyleSheet("""
            QTextEdit {
                background-color: #1f1f30;
                color: #ecf0f1;
                border: none;
                padding: 20px;
                font-size: 13px;
                line-height: 1.6;
            }
        """)
        self.text_display.setFrameStyle(QFrame.NoFrame)

        scroll_area.setWidget(self.text_display)
        scroll_area.setMinimumHeight(380)
        scroll_area.setMaximumHeight(480)
        scroll_area.setMinimumWidth(750)
        scroll_area.setMaximumWidth(850)

        # Center the scroll area
        scroll_container = QHBoxLayout()
        scroll_container.addStretch()
        scroll_container.addWidget(scroll_area)
        scroll_container.addStretch()
        layout.addLayout(scroll_container)

        layout.addSpacing(25)

        # Buttons Layout
        btn_layout = QHBoxLayout()

        self.btn_disagree = ModernButton("Tidak Setuju", danger=True)
        self.btn_disagree.clicked.connect(lambda: self.agreement_signal.emit(False))
        btn_layout.addWidget(self.btn_disagree)

        btn_layout.addStretch()

        self.btn_agree = ModernButton("Setuju dan Lanjutkan", primary=True)
        self.btn_agree.clicked.connect(lambda: self.agreement_signal.emit(True))
        btn_layout.addWidget(self.btn_agree)

        layout.addLayout(btn_layout)
        layout.addStretch()

class PaymentStage(StageWidget):
    """WhatsApp-based Payment Stage - Simple photo + unlock code workflow"""
    payment_verified = Signal(object)  # Emits order when unlocked
    payment_cancelled = Signal()
    skip_payment_clicked = Signal()  # For compatibility

    def __init__(self):
        self.tx_id = ""
        self.package_name = ""
        self.amount = 0
        self.families_count = 0
        self.members_count = 0
        self.files_path = ""
        super().__init__("💳 Pembayaran via WhatsApp")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Import here to avoid circular imports
        from .whatsapp_payment_stage import WhatsAppPaymentStage, WhatsAppOrderManager

        self.order_manager = WhatsAppOrderManager()
        self.whatsapp_widget = WhatsAppPaymentStage()
        self.whatsapp_widget.payment_verified.connect(self._on_verified)
        self.whatsapp_widget.payment_cancelled.connect(self._on_cancelled)

        layout.addWidget(self.whatsapp_widget)

    def set_payment_info(self, tx_id: str, package: str, amount: int,
                        families_count: int = 0, members_count: int = 0,
                        files_path: str = ""):
        """Set payment details for display."""
        self.tx_id = tx_id
        self.package_name = package
        self.amount = amount
        self.families_count = families_count
        self.members_count = members_count
        self.files_path = files_path

        # Pass to inner widget
        self.whatsapp_widget.set_order(
            tx_id, package, amount, families_count, members_count, files_path
        )

    def _on_verified(self, order):
        """Handle successful unlock."""
        self.payment_verified.emit(order)

    def _on_cancelled(self):
        """Handle cancellation."""
        self.payment_cancelled.emit()

    def load_payment_url(self, url, package, amount):
        """Compatibility method - does nothing in new system."""
        pass

    def cleanup(self):
        """No cleanup needed."""
        pass



class SuccessStage(StageWidget):
    def __init__(self):
        super().__init__("✅ Berhasil")

    def setup_ui(self):
        super().setup_ui()
        layout = self.layout()

        # Success icon
        icon = QLabel("🎉")
        icon.setStyleSheet("font-size: 80px;")
        icon.setAlignment(Qt.AlignCenter)
        layout.addWidget(icon)

        # Message
        self.message_label = QLabel()
        self.message_label.setStyleSheet("font-size: 18px; color: #27ae60; margin: 20px;")
        self.message_label.setAlignment(Qt.AlignCenter)
        self.message_label.setWordWrap(True)
        layout.addWidget(self.message_label)

        # Output folder
        self.folder_label = QLabel()
        self.folder_label.setStyleSheet("font-size: 13px; color: #95a5a6; margin: 10px; padding: 10px; background: #252538; border-radius: 6px;")
        self.folder_label.setAlignment(Qt.AlignCenter)
        self.folder_label.setWordWrap(True)
        layout.addWidget(self.folder_label)

        layout.addSpacing(20)

        # Open Folder Button
        self.open_btn = ModernButton("Buka Folder Output", primary=True)
        self.open_btn.clicked.connect(self.open_folder)
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(self.open_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        layout.addStretch()

    def set_success(self, output_folder):
        self.output_folder = output_folder
        self.message_label.setText("File berhasil dibuka! Struktur baru telah dirapikan.")
        csv_dir = os.path.join(output_folder, "csv")
        pdf_dir = os.path.join(output_folder, "pdf")
        xlsx_files = []
        try:
            xlsx_files = [f for f in os.listdir(output_folder) if f.lower().endswith('.xlsx')]
        except OSError:
            pass
        detail_lines = []
        if xlsx_files:
            detail_lines.append("📊 XLSX di root:")
            detail_lines.extend([f"  • {f}" for f in xlsx_files])
        if os.path.isdir(csv_dir):
            csv_count = sum(1 for f in os.listdir(csv_dir) if f.lower().endswith('.csv'))
            detail_lines.append(f"📁 CSV Folder: {csv_dir} ({csv_count} file)")
        if os.path.isdir(pdf_dir):
            pdf_count = sum(1 for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf'))
            detail_lines.append(f"📄 PDF Folder: {pdf_dir} ({pdf_count} file)")
        if not detail_lines:
            detail_lines.append(f"📂 Output: {output_folder}")
        self.folder_label.setText("\n".join(detail_lines))

    def open_folder(self):
        if hasattr(self, 'output_folder'):
            os.startfile(self.output_folder)
