#!/usr/bin/env python3
"""
app_gui.py - Modern Animated GUI with Embedded Payment (Indonesian Version)
Refactored to use modular architecture from gui package
"""

import os
import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QMessageBox
)
from PySide6.QtCore import Qt, Slot, QPropertyAnimation, QEasingCurve, QTimer, QPoint
from PySide6.QtGui import QPalette, QColor, QFont, QIcon

# Import from modular gui package
from gui import (
    StepIndicator, ModernButton,
    ConfigStage, PreScrapeConfirmStage, ProcessingStage,
    PackageStage, TermsStage, SuccessStage, PaymentStage,
    ScraperWorker, show_error_dialog, AppError
)

from orchestrator import Orchestrator


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DTSEN Downloader v1.0")

        # Set Window Icon
        if os.path.exists("assets/app_icon.png"):
            self.setWindowIcon(QIcon("assets/app_icon.png"))

        # Dynamic sizing: 2/3 of available screen geometry
        screen = QApplication.primaryScreen()
        if screen:
            rect = screen.availableGeometry()
            target_w = int(rect.width() * 2 / 3)
            target_h = int(rect.height() * 2 / 3)

            # Center the window
            x = (rect.width() - target_w) // 2
            y = (rect.height() - target_h) // 2

            self.setGeometry(x, y, target_w, target_h)
        else:
            # Fallback
            self.resize(1180, 750)

        self.setMinimumSize(840, 600)

        self.orch = Orchestrator()

        self.setup_ui()
        self.connect_signals()  # Moved after setup_ui so payment_stage exists
        self.setup_shortcuts()

    def setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        # --- STEP INDICATOR ---
        # Steps: Config -> Confirm -> Processing -> Package -> Terms -> Payment -> Success
        # Mapped indices: 0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6
        step_names = ["Config", "Confirm", "Processing", "Package", "Terms", "Payment", "Success"]
        self.step_indicator = StepIndicator(step_names)

        # Container for step indicator with padding
        step_container = QWidget()
        step_container.setStyleSheet("background-color: #1a1a2e; border-bottom: 1px solid #2c3e50;")
        step_layout = QVBoxLayout(step_container)
        step_layout.setContentsMargins(0, 10, 0, 10)
        step_layout.addWidget(self.step_indicator)

        layout.addWidget(step_container)

        # Stacked widget for stages
        self.stack = QStackedWidget()

        # Create stages
        self.config_stage = ConfigStage()  # Manual config input (no browser)
        self.confirm_stage = PreScrapeConfirmStage()
        self.processing_stage = ProcessingStage()
        self.package_stage = PackageStage()
        self.terms_stage = TermsStage()
        self.payment_stage = PaymentStage()  # WhatsApp payment with unlock code
        self.success_stage = SuccessStage()

        self.stack.addWidget(self.config_stage)       # 0 - Config Stage
        self.stack.addWidget(self.confirm_stage)      # 1 - Confirm
        self.stack.addWidget(self.processing_stage)   # 2 - Processing
        self.stack.addWidget(self.package_stage)      # 3 - Package
        self.stack.addWidget(self.terms_stage)        # 4 - Terms
        self.stack.addWidget(self.payment_stage)      # 5 - WhatsApp Payment
        self.stack.addWidget(self.success_stage)      # 6 - Success

        layout.addWidget(self.stack, 1)

        # Bottom bar with navigation
        bottom_bar = QWidget()
        bottom_bar.setStyleSheet("background: #1a1a2e; padding: 15px; border-top: 1px solid #2c3e50;")
        self.bottom_layout = QHBoxLayout(bottom_bar)
        self.bottom_layout.setContentsMargins(20, 10, 20, 10)

        # Left Button (Back / Clear)
        self.left_btn = ModernButton("Bersihkan", danger=True)
        self.left_btn.clicked.connect(self.on_left_btn)
        self.bottom_layout.addWidget(self.left_btn)

        self.bottom_layout.addStretch()

        # Right Button (Next / Confirm / Open)
        self.right_btn = ModernButton("Lanjutkan", primary=True)
        self.right_btn.clicked.connect(self.on_right_btn)
        self.bottom_layout.addWidget(self.right_btn)

        layout.addWidget(bottom_bar)

        # Connect config stage signals
        self.config_stage.capture_complete.connect(self.on_config_complete)

        # Connect package selection
        self.package_stage.package_selected.connect(self.on_package_selected)

        # Connect families_extracted to pass pre-captured families for optimized scraping
        self.config_stage.families_extracted.connect(self.on_families_extracted)

        # Connect terms agreement
        self.terms_stage.agreement_signal.connect(self.on_terms_agreement)

        # Initial State
        self.update_buttons(0)

    def connect_signals(self):
        self.orch.section_progress.connect(self.on_progress)
        self.orch.stage_changed.connect(self.on_stage_changed)
        self.orch.error_occurred.connect(self.on_error)
        self.orch.package_selection_requested.connect(self.show_package_stage)
        self.orch.manual_payment_requested.connect(self.on_payment_requested)
        self.orch.success_completed.connect(self.on_success)
        self.orch.metrics_updated.connect(self.on_metrics_updated)

        # Connect WhatsApp payment stage signals
        self.payment_stage.payment_verified.connect(self.on_payment_verified)
        self.payment_stage.payment_cancelled.connect(self.on_payment_cancelled)

    def slide_to_stage(self, index):
        # SLIDING ANIMATION
        current_widget = self.stack.currentWidget()
        next_widget = self.stack.widget(index)

        # Determine direction
        current_index = self.stack.currentIndex()
        direction = 1 if index > current_index else -1

        offset_x = self.stack.width() * direction

        # Set next widget geometry to off-screen
        next_widget.setGeometry(offset_x, 0, self.stack.width(), self.stack.height())

        # Animation for next widget (Slide In)
        anim_in = QPropertyAnimation(next_widget, b"pos")
        anim_in.setDuration(500)
        anim_in.setStartValue(QPoint(offset_x, 0))
        anim_in.setEndValue(QPoint(0, 0))
        anim_in.setEasingCurve(QEasingCurve.OutCubic)

        # Animation for current widget (Slide Out)
        # Note: QStackedWidget doesn't support parallel animations easily without trickery.
        # Simple approach: Just set current index, but QStackedWidget switches instantly.
        # Better approach for QStackedWidget: subclass or use external library.
        # Standard approach: Just use fade or simple switch if complex.
        # BUT, user asked for "sleek and elegant sliding animation".

        # Since standard QStackedWidget is tricky to animate directly without custom paint/geometry manips,
        # we'll just set the index and update the step indicator for now to ensure functionality first,
        # OR implementation of a simple transition:

        self.stack.setCurrentIndex(index) # Basic switch for stability
        self.step_indicator.set_step(index)
        self.update_buttons(index)

    def update_buttons(self, index):
        # Default state
        self.left_btn.setVisible(True)
        self.right_btn.setVisible(True)
        self.right_btn.setEnabled(True)
        self.right_btn.setDisabled(False)

        if index == 0:  # Config Stage
            self.left_btn.setVisible(False)
            self.right_btn.setText("Lanjutkan")
            self.right_btn.primary = True
            self.right_btn.update_style()

        elif index == 1:  # Confirm
            self.left_btn.setText("Kembali")
            self.left_btn.danger = False
            self.left_btn.update_style()

            self.right_btn.setText("Konfirmasi")
            self.right_btn.primary = True
            self.right_btn.update_style()

        elif index == 2:  # Processing
            self.left_btn.setVisible(False)
            self.right_btn.setEnabled(False)
            self.right_btn.setText("Memproses...")

        elif index == 3:  # Package
            self.left_btn.setVisible(False)
            self.right_btn.setVisible(False)  # Selection triggers next

        elif index == 4:  # Terms
            self.left_btn.setVisible(False)
            self.right_btn.setVisible(False)  # Buttons inside stage

        elif index == 5:  # WhatsApp Payment
            self.left_btn.setVisible(False)
            self.right_btn.setVisible(False)  # Buttons inside stage

        elif index == 6:  # Success
            self.left_btn.setVisible(False)
            self.right_btn.setVisible(False)  # Button moved to center

    def on_left_btn(self):
        current = self.stack.currentIndex()
        if current == 1:  # Confirm - Back to Config
            self.slide_to_stage(0)

    @Slot(str, list, int)
    def on_config_complete(self, authorization: str, entities: list, total_families: int):
        """Handle successful config save - DO NOT auto-navigate; wait for 'Lanjutkan'"""
        # Save config
        self.orch.save_config(authorization, "\n".join(entities), None)

        # Update confirm stage count (will be shown when user clicks 'Lanjutkan')
        self.confirm_stage.set_count(len(entities), total_families)

        # Do NOT slide automatically; user uses 'Lanjutkan' to go to next page

    def on_right_btn(self):
        current = self.stack.currentIndex()

        if current == 0:  # Config -> Go to Confirm (if config exists)
            # Smart behavior: Check if validation has been done
            if hasattr(self.config_stage, 'last_validated_total'):
                # Validation already done, proceed normally
                config = self.orch.load_config()
                if config and config.get("bearer_token") and config.get("entity_lines"):
                    lines = len([l for l in config["entity_lines"].splitlines() if l.strip()])
                    family_count = self.config_stage.last_validated_total

                    # Pass both entity count and family count
                    self.confirm_stage.set_count(lines, family_count)
                    self.slide_to_stage(1)
                else:
                    self.on_error("Tidak ada konfigurasi tersimpan. Silakan isi Authorization dan Entity terlebih dahulu.")
            else:
                # Validation NOT done yet - trigger validation first, then auto-navigate
                config = self.orch.load_config()
                if config and config.get("bearer_token") and config.get("entity_lines"):
                    # Set flag for auto-navigation after successful validation
                    self.config_stage._auto_navigate_after_validation = True

                    # Define callback for navigation
                    def navigate_to_confirm():
                        lines = len([l for l in config["entity_lines"].splitlines() if l.strip()])
                        family_count = self.config_stage.last_validated_total
                        self.confirm_stage.set_count(lines, family_count)
                        self.slide_to_stage(1)

                    self.config_stage._navigate_callback = navigate_to_confirm

                    # Trigger validation
                    self.config_stage.save_and_validate()
                else:
                    self.on_error("Tidak ada konfigurasi tersimpan. Silakan isi Authorization dan Entity terlebih dahulu.")

        elif current == 1:  # Confirm -> Process
            # Update custom folder before processing
            selected_folder = self.confirm_stage.get_selected_folder()
            if selected_folder:
                self.orch.custom_output_folder = selected_folder
            # Only proceed to next page when user clicks 'Lanjutkan'
            self.slide_to_stage(2)  # Processing stage
            self.start_processing()

        elif current == 7:  # Success -> Open
            self.success_stage.open_folder()

    def start_processing(self):
        if not os.getenv("AES_BASE64_KEY"):
            self.on_error("❌ AES_BASE64_KEY belum diatur!")
            return

        self.processing_stage.update_status("🚀 Memulai...")
        self.processing_stage.start_timer()
        self.worker = ScraperWorker(self.orch)
        self.worker.finished.connect(self.on_scrape_finished)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    @Slot(str, int, int)
    def on_progress(self, section, current, total):
        self.processing_stage.update_progress(current, total)
        self.processing_stage.add_log(f"⏳ {section}: {current}/{total}")

    @Slot(str)
    def on_stage_changed(self, stage):
        self.processing_stage.update_status(stage)
        self.processing_stage.add_log(f"🔸 {stage}")

    @Slot(float, int)
    def on_metrics_updated(self, speed, eta_seconds):
        """Update real-time scraping metrics"""
        self.processing_stage.update_metrics(speed, eta_seconds)

    def on_scrape_finished(self, result):
        self.processing_stage.stop_timer()
        self.processing_stage.add_log("✅ Pemrosesan selesai!")

    @Slot(int, int)
    def show_package_stage(self, families, members):
        self.package_stage.set_counts(families, members)
        self.slide_to_stage(3)  # Package stage

    @Slot(list)
    def on_families_extracted(self, families):
        """Store pre-captured families in orchestrator for optimized scraping"""
        self.orch.pre_captured_families = families
        print(f"[MainWindow] Received {len(families)} pre-captured families")

    def on_package_selected(self, package):
        # Orchestrator creates payment immediately, BUT we want to show Terms first.
        # We just store the package and move to Terms stage.
        self.selected_package_temp = package
        self.slide_to_stage(4)  # Go to Terms

    def on_terms_agreement(self, agreed):
        if agreed:
            # Proceed to create payment
            if hasattr(self, 'selected_package_temp'):
                self.orch.handle_package_selected(self.selected_package_temp)
        else:
            # Disagree: Home or Exit
            msg = QMessageBox()
            msg.setWindowTitle("Konfirmasi")
            msg.setText("Anda tidak menyetujui Syarat & Ketentuan.")
            msg.setInformativeText("Pilih tindakan selanjutnya:")
            btn_home = msg.addButton("Halaman Utama", QMessageBox.ActionRole)
            btn_exit = msg.addButton("Keluar Aplikasi", QMessageBox.ActionRole)
            msg.exec()

            if msg.clickedButton() == btn_home:
                self.slide_to_stage(0)  # Config stage
            else:
                QApplication.quit()

    @Slot(object)
    def on_payment_requested(self, order):
        """Handle payment request - show WhatsApp payment stage"""
        self.slide_to_stage(5)  # WhatsApp Payment Stage
        # Get TX-ID from orchestrator
        tx_id = getattr(self.orch, 'current_tx_id', order.order_id if hasattr(order, 'order_id') else 'TX-UNKNOWN')
        self.payment_stage.set_payment_info(
            tx_id,
            order.package_name,
            order.amount,
            order.families_count,
            order.members_count,
            order.files_path
        )

    @Slot(object)
    def on_payment_verified(self, order):
        """Handle successful unlock - proceed to success"""
        self.orch.handle_unlock_verified(order)

    def on_payment_cancelled(self):
        """Handle payment cancellation - return to config"""
        self.slide_to_stage(0)

    @Slot(str)
    def on_success(self, output_folder):
        self.success_stage.set_success(output_folder)
        self.slide_to_stage(6)  # Success Stage (now index 6)

    @Slot(object)
    def on_error(self, error):
        error_msg = str(error)

        if "401" in error_msg or "Unauthorized" in error_msg:
            self._handle_unauthorized_error(error_msg)
            return

        # If in processing, show log
        if self.stack.currentIndex() == 2:  # Processing stage
            self.processing_stage.add_log(f"❌ {error_msg}")

            # Show dialog for critical errors
            if isinstance(error, AppError):
                show_error_dialog(self, error, "Error Processing")

            # Delay return to start
            QTimer.singleShot(4000, lambda: self.slide_to_stage(0))
        else:
            if self.stack.currentIndex() == 0:  # Config
                print(f"ERROR: {error_msg}")
                show_error_dialog(self, error, "Error")
            else:
                self.processing_stage.add_log(f"❌ {error_msg}")
                show_error_dialog(self, error, "Error")
                QTimer.singleShot(4000, lambda: self.slide_to_stage(0))

    def _handle_unauthorized_error(self, error):
        if self.stack.currentIndex() == 2:  # Processing stage
            self.processing_stage.add_log(f"❌ Autentikasi gagal: {error}")
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Warning)
        dialog.setWindowTitle("Autentikasi Gagal (401)")
        dialog.setText("Permintaan API ditolak (401 Unauthorized).")
        dialog.setInformativeText(
            "Kemungkinan Authorisasi atau entitas yang dimasukkan tidak valid. Silakan lakukan Auto-Capture ulang."
        )
        retry_btn = dialog.addButton("Auto-Capture Ulang", QMessageBox.AcceptRole)
        exit_btn = dialog.addButton("Keluar", QMessageBox.RejectRole)
        dialog.setDefaultButton(retry_btn)
        dialog.exec()
        if dialog.clickedButton() == retry_btn:
            self.slide_to_stage(0)
        else:
            QApplication.quit()

    def setup_shortcuts(self):
        from PySide6.QtGui import QAction, QKeySequence

        # Quit
        quit_action = QAction("Quit", self)
        quit_action.setShortcut(QKeySequence("Ctrl+Q"))
        quit_action.triggered.connect(self.close)
        self.addAction(quit_action)

        # Fullscreen toggle (F11)
        fs_action = QAction("Toggle Fullscreen", self)
        fs_action.setShortcut(QKeySequence("F11"))
        fs_action.triggered.connect(self.toggle_fullscreen)
        self.addAction(fs_action)

    def toggle_fullscreen(self):
        if self.isFullScreen():
            self.showNormal()
        else:
            self.showFullScreen()

def main():
    # Suppress Qt WebEngine DirectComposition warnings on Windows
    os.environ["QT_LOGGING_RULES"] = "qt.qpa.windows=false"
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = "--disable-gpu"

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Dark palette
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(30, 30, 46))
    palette.setColor(QPalette.WindowText, Qt.white)
    palette.setColor(QPalette.Base, QColor(24, 24, 36))
    palette.setColor(QPalette.AlternateBase, QColor(30, 30, 46))
    palette.setColor(QPalette.Text, Qt.white)
    palette.setColor(QPalette.Button, QColor(30, 30, 46))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    app.setPalette(palette)

    # Font
    font = QFont("Segoe UI", 10)
    app.setFont(font)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
