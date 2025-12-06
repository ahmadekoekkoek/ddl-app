"""
Twilio SMS Handler
Parses incoming Twilio SMS messages for unlock codes.

Expected SMS format:
    Seseorang sudah berhasil melakukan proses scraping!
    Paket pilihan: [Basic/PRO]
    ID transaksi: <tx-id>
    Jumlah Keluarga: <family count>
    Total yang harus dibayar : <bill amount>

    Kode unlock untuk transaksi ini adalah: <unlock code>
"""

import os
import re
from typing import Optional, Dict, Tuple
from dataclasses import dataclass

# Twilio imports
try:
    from twilio.rest import Client
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False


@dataclass
class ParsedUnlockSMS:
    """Parsed data from unlock SMS."""
    package: str
    tx_id: str
    family_count: int
    amount: int
    unlock_code: str
    raw_message: str


class TwilioSMSHandler:
    """Handles Twilio SMS for receiving unlock codes."""

    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.verify_sid = os.getenv("TWILIO_VERIFY_SID", "")
        self.from_number = os.getenv("TWILIO_FROM", "")

        self._client = None

    @property
    def is_configured(self) -> bool:
        """Check if Twilio is properly configured."""
        return bool(self.account_sid and self.auth_token)

    @property
    def client(self):
        """Get Twilio client (lazy initialization)."""
        if self._client is None and TWILIO_AVAILABLE and self.is_configured:
            self._client = Client(self.account_sid, self.auth_token)
        return self._client

    def parse_unlock_sms(self, message_body: str) -> Optional[ParsedUnlockSMS]:
        """
        Parse incoming SMS to extract unlock code information.

        Expected format:
            Seseorang sudah berhasil melakukan proses scraping!
            Paket pilihan: [Basic/PRO]
            ID transaksi: TX-XXXXXX
            Jumlah Keluarga: XX
            Total yang harus dibayar : Rp X.XXX.XXX

            Kode unlock untuk transaksi ini adalah: XXXXXX
        """
        try:
            # Extract package type
            package_match = re.search(r'Paket pilihan:\s*\[?(\w+)\]?', message_body, re.IGNORECASE)
            package = package_match.group(1) if package_match else "Unknown"

            # Extract TX-ID
            tx_match = re.search(r'ID transaksi:\s*(TX-[\w-]+)', message_body, re.IGNORECASE)
            tx_id = tx_match.group(1) if tx_match else ""

            # Extract family count
            family_match = re.search(r'Jumlah Keluarga:\s*(\d+)', message_body, re.IGNORECASE)
            family_count = int(family_match.group(1)) if family_match else 0

            # Extract amount (handle Rp formatting)
            amount_match = re.search(r'Total yang harus dibayar\s*:\s*(?:Rp\.?\s*)?([\d.,]+)', message_body, re.IGNORECASE)
            if amount_match:
                amount_str = amount_match.group(1).replace(".", "").replace(",", "")
                amount = int(amount_str)
            else:
                amount = 0

            # Extract unlock code
            code_match = re.search(r'Kode unlock untuk transaksi ini adalah:\s*(\w+)', message_body, re.IGNORECASE)
            unlock_code = code_match.group(1) if code_match else ""

            if not tx_id or not unlock_code:
                return None

            return ParsedUnlockSMS(
                package=package,
                tx_id=tx_id,
                family_count=family_count,
                amount=amount,
                unlock_code=unlock_code,
                raw_message=message_body
            )

        except Exception as e:
            print(f"Error parsing SMS: {e}")
            return None

    def fetch_recent_messages(self, limit: int = 10) -> list:
        """Fetch recent incoming SMS messages from Twilio."""
        if not self.client:
            return []

        try:
            messages = self.client.messages.list(
                to=self.from_number,
                limit=limit
            )
            return [
                {
                    "sid": m.sid,
                    "from": m.from_,
                    "body": m.body,
                    "date_sent": m.date_sent.isoformat() if m.date_sent else None,
                    "status": m.status
                }
                for m in messages
            ]
        except Exception as e:
            print(f"Error fetching Twilio messages: {e}")
            return []

    def check_for_unlock_code(self, tx_id: str) -> Optional[str]:
        """
        Check recent messages for an unlock code matching the given TX-ID.
        Returns the unlock code if found, None otherwise.
        """
        messages = self.fetch_recent_messages(limit=20)

        for msg in messages:
            parsed = self.parse_unlock_sms(msg.get("body", ""))
            if parsed and parsed.tx_id.upper() == tx_id.upper():
                return parsed.unlock_code

        return None


def register_unlock_code_from_sms(sms_body: str, order_manager) -> Tuple[bool, str]:
    """
    Parse SMS and register unlock code in the order database.

    Args:
        sms_body: Raw SMS message body
        order_manager: WhatsAppOrderManager instance

    Returns:
        Tuple of (success: bool, message: str)
    """
    handler = TwilioSMSHandler()
    parsed = handler.parse_unlock_sms(sms_body)

    if not parsed:
        return False, "Failed to parse SMS message"

    if not parsed.tx_id or not parsed.unlock_code:
        return False, "SMS missing TX-ID or unlock code"

    # Register in order manager
    success = order_manager.set_unlock_code(parsed.tx_id, parsed.unlock_code)

    if success:
        return True, f"Unlock code registered for {parsed.tx_id}"
    else:
        return False, f"Order {parsed.tx_id} not found in database"
