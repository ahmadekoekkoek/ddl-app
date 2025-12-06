"""
Twilio Notification Service
Sends WhatsApp notifications to seller when orders are created.
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Try to import Twilio
try:
    from twilio.rest import Client
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False
    print("⚠️ Twilio not installed. Run: pip install twilio")


# Seller's WhatsApp number (receives notifications)
SELLER_WHATSAPP = os.getenv("SELLER_WHATSAPP", "6282331836926")  # Indonesian format


@dataclass
class OrderNotification:
    """Data for order notification message."""
    package: str
    tx_id: str
    family_count: int
    amount: int
    unlock_code: str


class TwilioNotifier:
    """Sends WhatsApp notifications via Twilio."""

    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.from_number = os.getenv("TWILIO_FROM", "")  # Twilio WhatsApp/SMS number

        self._client = None

    @property
    def is_configured(self) -> bool:
        """Check if Twilio is properly configured."""
        return bool(self.account_sid and self.auth_token and self.from_number)

    @property
    def client(self):
        """Get Twilio client (lazy initialization)."""
        if self._client is None and TWILIO_AVAILABLE and self.is_configured:
            self._client = Client(self.account_sid, self.auth_token)
        return self._client

    def format_notification_message(self, order: OrderNotification) -> str:
        """Format the notification message for seller."""
        return f"""Seseorang sudah berhasil melakukan proses scraping!
Paket pilihan: {order.package}
ID transaksi: {order.tx_id}
Jumlah Keluarga: {order.family_count}
Total yang harus dibayar : Rp {order.amount:,}

Kode unlock untuk transaksi ini adalah: {order.unlock_code}""".replace(",", ".")

    def send_whatsapp(self, to_number: str, message: str) -> tuple[bool, str]:
        """
        Send WhatsApp message via Twilio.

        Args:
            to_number: Recipient number in format 62XXXXXXXXXX
            message: Message body

        Returns:
            Tuple of (success, message_sid or error)
        """
        if not TWILIO_AVAILABLE:
            return False, "Twilio library not installed"

        if not self.is_configured:
            return False, "Twilio not configured (check .env)"

        try:
            # Format for WhatsApp
            whatsapp_to = f"whatsapp:+{to_number}"
            whatsapp_from = f"whatsapp:{self.from_number}"

            msg = self.client.messages.create(
                body=message,
                from_=whatsapp_from,
                to=whatsapp_to
            )

            print(f"✅ WhatsApp sent to +{to_number}: {msg.sid}")
            return True, msg.sid

        except Exception as e:
            error_msg = str(e)
            print(f"❌ WhatsApp send failed: {error_msg}")
            return False, error_msg

    def send_sms(self, to_number: str, message: str) -> tuple[bool, str]:
        """
        Send SMS via Twilio (fallback if WhatsApp fails).

        Args:
            to_number: Recipient number in format 62XXXXXXXXXX
            message: Message body

        Returns:
            Tuple of (success, message_sid or error)
        """
        if not TWILIO_AVAILABLE:
            return False, "Twilio library not installed"

        if not self.is_configured:
            return False, "Twilio not configured (check .env)"

        try:
            msg = self.client.messages.create(
                body=message,
                from_=self.from_number,
                to=f"+{to_number}"
            )

            print(f"✅ SMS sent to +{to_number}: {msg.sid}")
            return True, msg.sid

        except Exception as e:
            error_msg = str(e)
            print(f"❌ SMS send failed: {error_msg}")
            return False, error_msg

    def notify_seller(self, order: OrderNotification,
                      seller_number: str = None) -> tuple[bool, str]:
        """
        Send order notification to seller via WhatsApp.

        Args:
            order: Order notification data
            seller_number: Override seller number (default: from env)

        Returns:
            Tuple of (success, message_sid or error)
        """
        to_number = seller_number or SELLER_WHATSAPP
        message = self.format_notification_message(order)

        print(f"📲 Sending notification to seller: +{to_number}")
        print(f"📝 Message:\n{message}")

        # Try WhatsApp first, fallback to SMS
        success, result = self.send_whatsapp(to_number, message)

        if not success:
            print("⚠️ WhatsApp failed, trying SMS...")
            success, result = self.send_sms(to_number, message)

        return success, result


def send_order_notification(tx_id: str, package: str, family_count: int,
                           amount: int, unlock_code: str,
                           seller_number: str = None) -> tuple[bool, str]:
    """
    Convenience function to send order notification to seller.

    Returns:
        Tuple of (success, message_sid or error)
    """
    notifier = TwilioNotifier()

    order = OrderNotification(
        package=package,
        tx_id=tx_id,
        family_count=family_count,
        amount=amount,
        unlock_code=unlock_code
    )

    return notifier.notify_seller(order, seller_number)
