"""
WhatsApp Payment Stage - Simplified Payment System
User sends photo of payment to WhatsApp, receives unlock code via SMS.
"""

import os
import sqlite3
import secrets
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict
from enum import Enum

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QFrame, QMessageBox, QApplication, QScrollArea
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont

from .widgets import ModernButton


# =============================================================================
# CONFIGURATION
# =============================================================================

SELLER_WHATSAPP_DISPLAY = "0823-3183-6926"  # Display format (no link)


# =============================================================================
# DATA MODELS
# =============================================================================

class OrderStatus(Enum):
    PENDING = "pending"         # Waiting for payment
    PAID = "paid"               # User claims paid
    UNLOCKED = "unlocked"       # Files unlocked
    CANCELLED = "cancelled"     # Order cancelled


@dataclass
class WhatsAppOrder:
    tx_id: str
    package_name: str
    amount: int
    status: OrderStatus
    created_at: str
    families_count: int = 0
    members_count: int = 0
    unlock_code: str = ""
    unlocked_at: str = ""
    files_path: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d['status'] = self.status.value
        return d


# =============================================================================
# ORDER MANAGER (SQLite)
# =============================================================================

class WhatsAppOrderManager:
    """Manages orders in local SQLite database."""

    def __init__(self, db_path: str = "whatsapp_orders.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    tx_id TEXT PRIMARY KEY,
                    package_name TEXT,
                    amount INTEGER,
                    status TEXT,
                    created_at TEXT,
                    families_count INTEGER,
                    members_count INTEGER,
                    unlock_code TEXT,
                    unlocked_at TEXT,
                    files_path TEXT
                )
            """)
            conn.commit()

    def create_order(self, tx_id: str, package_name: str, amount: int,
                     families_count: int = 0, members_count: int = 0,
                     files_path: str = "") -> WhatsAppOrder:
        """Create new order."""
        order = WhatsAppOrder(
            tx_id=tx_id,
            package_name=package_name,
            amount=amount,
            status=OrderStatus.PENDING,
            created_at=datetime.now().isoformat(),
            families_count=families_count,
            members_count=members_count,
            files_path=files_path
        )
        self._save_order(order)
        return order

    def _save_order(self, order: WhatsAppOrder):
        """Save order to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO orders VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
                order.tx_id, order.package_name, order.amount,
                order.status.value, order.created_at,
                order.families_count, order.members_count,
                order.unlock_code, order.unlocked_at, order.files_path
            ))
            conn.commit()

    def get_order(self, tx_id: str) -> Optional[WhatsAppOrder]:
        """Get order by TX-ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM orders WHERE tx_id = ?", (tx_id,)
            )
            row = cur.fetchone()
            if row:
                return WhatsAppOrder(
                    tx_id=row['tx_id'],
                    package_name=row['package_name'],
                    amount=row['amount'],
                    status=OrderStatus(row['status']),
                    created_at=row['created_at'],
                    families_count=row['families_count'] or 0,
                    members_count=row['members_count'] or 0,
                    unlock_code=row['unlock_code'] or "",
                    unlocked_at=row['unlocked_at'] or "",
                    files_path=row['files_path'] or ""
                )
        return None

    def set_unlock_code(self, tx_id: str, unlock_code: str) -> bool:
        """Set unlock code for an order (called when SMS is received)."""
        order = self.get_order(tx_id)
        if order:
            order.unlock_code = unlock_code
            self._save_order(order)
            return True
        return False

    def verify_unlock_code(self, tx_id: str, code: str) -> bool:
        """Verify unlock code and mark as unlocked."""
        order = self.get_order(tx_id)
        if order and order.unlock_code and order.unlock_code.upper() == code.upper():
            order.status = OrderStatus.UNLOCKED
            order.unlocked_at = datetime.now().isoformat()
            self._save_order(order)
            return True
        return False

    def update_order(self, order: WhatsAppOrder):
        """Update existing order."""
        self._save_order(order)


# =============================================================================
# WHATSAPP PAYMENT STAGE
# =============================================================================

class WhatsAppPaymentStage(QWidget):
    """Simplified payment screen - WhatsApp photo + unlock code."""

    payment_verified = Signal(object)  # Emits WhatsAppOrder when unlocked
    payment_cancelled = Signal()

    def __init__(self):
        super().__init__()
        self.order: Optional[WhatsAppOrder] = None
        self.order_manager = WhatsAppOrderManager()
        self.setup_ui()

    def setup_ui(self):
        # Main layout with scroll area for better UX
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("""
            QScrollArea {
                border: none;
                background: transparent;
            }
            QScrollBar:vertical {
                background: #252538;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #3498db;
                border-radius: 5px;
                min-height: 30px;
            }
        """)

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 10, 20, 20)

        # Main container

        # === ORDER INFO SECTION ===
        order_frame = QFrame()
        order_frame.setStyleSheet("""
            QFrame {
                background: rgba(41, 128, 185, 0.2);
                border: 2px solid #3498db;
                border-radius: 10px;
                padding: 15px;
            }
        """)
        order_layout = QVBoxLayout(order_frame)

        # TX-ID (large, prominent)
        self.tx_id_label = QLabel()
        self.tx_id_label.setStyleSheet("""
            font-size: 28px;
            font-weight: bold;
            color: #f39c12;
            padding: 10px;
        """)
        self.tx_id_label.setAlignment(Qt.AlignCenter)
        order_layout.addWidget(self.tx_id_label)

        # Copy TX-ID button
        copy_tx_btn = QPushButton("📋 Salin TX-ID")
        copy_tx_btn.setStyleSheet("""
            QPushButton {
                background: #f39c12;
                color: white;
                border: none;
                padding: 8px 15px;
                border-radius: 5px;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton:hover { background: #e67e22; }
        """)
        copy_tx_btn.clicked.connect(self._copy_tx_id)
        order_layout.addWidget(copy_tx_btn, alignment=Qt.AlignCenter)

        # Order details
        self.details_label = QLabel()
        self.details_label.setStyleSheet("""
            font-size: 14px;
            color: #ecf0f1;
            padding: 10px;
        """)
        self.details_label.setAlignment(Qt.AlignCenter)
        order_layout.addWidget(self.details_label)

        # Amount (large)
        self.amount_label = QLabel()
        self.amount_label.setStyleSheet("""
            font-size: 32px;
            font-weight: bold;
            color: #2ecc71;
            padding: 15px;
        """)
        self.amount_label.setAlignment(Qt.AlignCenter)
        order_layout.addWidget(self.amount_label)

        layout.addWidget(order_frame)

        # === PAYMENT INSTRUCTIONS SECTION ===
        instructions_frame = QFrame()
        instructions_frame.setStyleSheet("""
            QFrame {
                background: rgba(39, 174, 96, 0.15);
                border: 2px solid #27ae60;
                border-radius: 10px;
                padding: 15px;
            }
        """)
        instructions_layout = QVBoxLayout(instructions_frame)

        # Title
        inst_title = QLabel("📱 Cara Pembayaran:")
        inst_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #2ecc71;")
        instructions_layout.addWidget(inst_title)

        # Steps
        steps_text = """
        <ol style="color: #ecf0f1; font-size: 13px; line-height: 1.8;">
            <li>Foto halaman ini yang menampilkan TX-ID beserta jumlah total</li>
            <li>Kirim ke nomor WhatsApp di bawah ini</li>
            <li>Tunggu kode unlock dikirim lewat WA</li>
            <li>Masukkan kode unlock di bawah</li>
        </ol>
        """
        steps_label = QLabel(steps_text)
        steps_label.setWordWrap(True)
        instructions_layout.addWidget(steps_label)

        # WhatsApp number (large, prominent)
        wa_title = QLabel("📲 Kirim ke WhatsApp:")
        wa_title.setStyleSheet("font-size: 14px; color: #bdc3c7; margin-top: 10px;")
        instructions_layout.addWidget(wa_title)

        wa_number = QLabel(SELLER_WHATSAPP_DISPLAY)
        wa_number.setStyleSheet("""
            font-size: 36px;
            font-weight: bold;
            color: #25D366;
            padding: 15px;
            background: rgba(37, 211, 102, 0.1);
            border-radius: 8px;
        """)
        wa_number.setAlignment(Qt.AlignCenter)
        wa_number.setTextInteractionFlags(Qt.TextSelectableByMouse)
        instructions_layout.addWidget(wa_number)

        # Note
        note_label = QLabel("⚠️ Sertakan TX-ID dalam pesan WhatsApp Anda!")
        note_label.setStyleSheet("""
            font-size: 12px;
            color: #e74c3c;
            font-weight: bold;
            padding: 8px;
        """)
        note_label.setAlignment(Qt.AlignCenter)
        instructions_layout.addWidget(note_label)

        layout.addWidget(instructions_frame)

        # === UNLOCK CODE SECTION ===
        unlock_frame = QFrame()
        unlock_frame.setStyleSheet("""
            QFrame {
                background: rgba(155, 89, 182, 0.15);
                border: 2px solid #9b59b6;
                border-radius: 10px;
                padding: 15px;
            }
        """)
        unlock_layout = QVBoxLayout(unlock_frame)

        unlock_title = QLabel("🔓 Masukkan Kode Unlock:")
        unlock_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #9b59b6;")
        unlock_layout.addWidget(unlock_title)

        unlock_help = QLabel("Kode unlock akan dikirim via SMS setelah pembayaran terverifikasi")
        unlock_help.setStyleSheet("font-size: 12px; color: #bdc3c7;")
        unlock_layout.addWidget(unlock_help)

        # Code input
        self.code_input = QLineEdit()
        self.code_input.setPlaceholderText("Masukkan kode unlock...")
        self.code_input.setMaxLength(20)
        self.code_input.setStyleSheet("""
            QLineEdit {
                background: #34495e;
                color: white;
                border: 2px solid #9b59b6;
                border-radius: 8px;
                padding: 15px;
                font-size: 24px;
                font-weight: bold;
                letter-spacing: 4px;
            }
            QLineEdit:focus {
                border-color: #2ecc71;
            }
        """)
        self.code_input.setAlignment(Qt.AlignCenter)
        self.code_input.returnPressed.connect(self._on_verify)
        unlock_layout.addWidget(self.code_input)

        layout.addWidget(unlock_frame)

        # === BUTTONS ===
        btn_layout = QHBoxLayout()

        self.cancel_btn = ModernButton("❌ Batalkan", primary=False)
        self.cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self.cancel_btn)

        btn_layout.addStretch()

        self.verify_btn = ModernButton("🔓 Verifikasi & Buka File", primary=True)
        self.verify_btn.setStyleSheet("""
            QPushButton {
                background: #27ae60;
                color: white;
                border: none;
                padding: 15px 40px;
                border-radius: 8px;
                font-size: 16px;
                font-weight: bold;
            }
            QPushButton:hover { background: #2ecc71; }
            QPushButton:pressed { background: #1e8449; }
        """)
        self.verify_btn.clicked.connect(self._on_verify)
        btn_layout.addWidget(self.verify_btn)

        layout.addLayout(btn_layout)
        layout.addStretch()

        # Complete scroll area setup
        scroll.setWidget(content)
        main_layout.addWidget(scroll)

    def set_order(self, tx_id: str, package_name: str, amount: int,
                  families_count: int = 0, members_count: int = 0,
                  files_path: str = ""):
        """Set current order details - fetch existing order (don't create new one!)."""
        # Fetch existing order created by orchestrator (includes unlock code)
        self.order = self.order_manager.get_order(tx_id)

        if not self.order:
            # Fallback: create order if not found (shouldn't happen normally)
            print(f"⚠️ Order {tx_id} not found, creating new one")
            self.order = self.order_manager.create_order(
                tx_id, package_name, amount, families_count, members_count, files_path
            )
        else:
            print(f"✅ Loaded existing order {tx_id} with unlock code")

        # Update UI
        self.tx_id_label.setText(f"📋 {tx_id}")
        self.details_label.setText(
            f"📦 Paket: <b>{package_name}</b> | "
            f"👥 {families_count} Keluarga"
        )
        self.amount_label.setText(f"Rp {amount:,}".replace(",", "."))
        self.code_input.clear()

    def set_existing_order(self, order: WhatsAppOrder):
        """Set an existing order (for unlock mode)."""
        self.order = order

        self.tx_id_label.setText(f"📋 {order.tx_id}")
        self.details_label.setText(
            f"📦 Paket: <b>{order.package_name}</b> | "
            f"👥 {order.families_count} Keluarga"
        )
        self.amount_label.setText(f"Rp {order.amount:,}".replace(",", "."))
        self.code_input.clear()

    def _copy_tx_id(self):
        """Copy TX-ID to clipboard."""
        if self.order:
            QApplication.clipboard().setText(self.order.tx_id)
            QMessageBox.information(
                self, "Tersalin",
                f"TX-ID '{self.order.tx_id}' telah disalin ke clipboard!"
            )

    def _on_verify(self):
        """Verify unlock code and unlock files."""
        if not self.order:
            return

        code = self.code_input.text().strip().upper()

        if not code:
            QMessageBox.warning(self, "Kode Kosong", "Mohon masukkan kode unlock.")
            return

        # Verify code
        if self.order_manager.verify_unlock_code(self.order.tx_id, code):
            self.order = self.order_manager.get_order(self.order.tx_id)
            QMessageBox.information(
                self, "Berhasil! 🎉",
                "Kode unlock valid!\n\nFile Anda akan segera dibuka."
            )
            self.payment_verified.emit(self.order)
        else:
            QMessageBox.warning(
                self, "Kode Tidak Valid",
                "Kode unlock tidak valid.\n\n"
                "Pastikan Anda memasukkan kode yang dikirim via SMS.\n"
                "Jika belum menerima kode, silakan hubungi via WhatsApp."
            )

    def _on_cancel(self):
        """Handle payment cancellation."""
        reply = QMessageBox.question(
            self, "Batalkan Pembayaran",
            "Yakin ingin membatalkan pembayaran?\n\n"
            "Data yang sudah di-scrape akan tetap tersimpan (terenkripsi).",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            if self.order:
                self.order.status = OrderStatus.CANCELLED
                self.order_manager.update_order(self.order)
            self.payment_cancelled.emit()
