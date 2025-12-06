"""
cloud_orders.py - Supabase-backed order management for cloud sync
Enables admin to verify payments from mobile dashboard
"""

import os
import secrets
from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass, asdict
from enum import Enum

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("Warning: supabase not installed. Run: pip install supabase")


class OrderStatus(Enum):
    PENDING = "pending"
    PAID = "paid"
    VERIFIED = "verified"  # Admin verified, ready to unlock
    UNLOCKED = "unlocked"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class CloudOrder:
    order_id: str
    package_name: str
    amount: int
    status: OrderStatus
    created_at: str
    tx_id: str = ""  # Transaction ID for file unlocking
    user_phone: str = ""
    payment_method: str = ""
    payment_ref: str = ""
    paid_at: str = ""
    verified_at: str = ""
    unlocked_at: str = ""
    files_path: str = ""
    families_count: int = 0
    members_count: int = 0

    def to_dict(self) -> Dict:
        d = asdict(self)
        d['status'] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: Dict) -> 'CloudOrder':
        d = d.copy()
        d['status'] = OrderStatus(d['status'])
        return cls(**d)


class CloudOrderManager:
    """Manages orders in Supabase cloud database."""

    TABLE_NAME = "orders"

    def __init__(self):
        self.supabase: Optional[Client] = None
        self._init_client()

    def _init_client(self):
        """Initialize Supabase client from environment variables."""
        if not SUPABASE_AVAILABLE:
            print("ERROR: supabase package not installed")
            return

        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")  # Use anon key for client, service key for admin

        if not url or not key:
            print("WARNING: SUPABASE_URL or SUPABASE_KEY not set")
            return

        try:
            self.supabase = create_client(url, key)
            print(f"✅ Supabase connected: {url[:30]}...")
        except Exception as e:
            print(f"ERROR: Failed to connect to Supabase: {e}")

    @property
    def is_connected(self) -> bool:
        return self.supabase is not None

    def generate_order_id(self) -> str:
        """Generate unique order ID."""
        date_part = datetime.now().strftime("%Y%m%d")
        random_part = secrets.token_hex(3).upper()
        return f"DTK-{date_part}-{random_part}"

    def create_order(self, package_name: str, amount: int, tx_id: str,
                     families_count: int = 0, members_count: int = 0,
                     files_path: str = "") -> Optional[CloudOrder]:
        """Create new order in cloud."""
        if not self.is_connected:
            print("ERROR: Not connected to Supabase")
            return None

        order = CloudOrder(
            order_id=self.generate_order_id(),
            package_name=package_name,
            amount=amount,
            status=OrderStatus.PENDING,
            created_at=datetime.now().isoformat(),
            tx_id=tx_id,
            families_count=families_count,
            members_count=members_count,
            files_path=files_path
        )

        try:
            self.supabase.table(self.TABLE_NAME).insert(order.to_dict()).execute()
            print(f"✅ Order created: {order.order_id}")
            return order
        except Exception as e:
            print(f"ERROR: Failed to create order: {e}")
            return None

    def get_order(self, order_id: str) -> Optional[CloudOrder]:
        """Get order by ID."""
        if not self.is_connected:
            return None

        try:
            result = self.supabase.table(self.TABLE_NAME).select("*").eq("order_id", order_id).execute()
            if result.data:
                return CloudOrder.from_dict(result.data[0])
        except Exception as e:
            print(f"ERROR: Failed to get order: {e}")
        return None

    def get_order_by_tx_id(self, tx_id: str) -> Optional[CloudOrder]:
        """Get order by transaction ID."""
        if not self.is_connected:
            return None

        try:
            result = self.supabase.table(self.TABLE_NAME).select("*").eq("tx_id", tx_id).execute()
            if result.data:
                return CloudOrder.from_dict(result.data[0])
        except Exception as e:
            print(f"ERROR: Failed to get order by tx_id: {e}")
        return None

    def update_order(self, order: CloudOrder) -> bool:
        """Update existing order."""
        if not self.is_connected:
            return False

        try:
            self.supabase.table(self.TABLE_NAME).update(order.to_dict()).eq("order_id", order.order_id).execute()
            return True
        except Exception as e:
            print(f"ERROR: Failed to update order: {e}")
            return False

    def mark_as_paid(self, order_id: str, payment_method: str,
                     payment_ref: str, user_phone: str) -> bool:
        """Mark order as paid (pending admin verification)."""
        order = self.get_order(order_id)
        if order:
            order.status = OrderStatus.PAID
            order.payment_method = payment_method
            order.payment_ref = payment_ref
            order.user_phone = user_phone
            order.paid_at = datetime.now().isoformat()
            return self.update_order(order)
        return False

    def verify_order(self, order_id: str) -> bool:
        """Admin verifies payment - triggers auto-unlock on user side."""
        order = self.get_order(order_id)
        if order:
            order.status = OrderStatus.VERIFIED
            order.verified_at = datetime.now().isoformat()
            return self.update_order(order)
        return False

    def mark_unlocked(self, order_id: str) -> bool:
        """Mark order as unlocked after files are decrypted."""
        order = self.get_order(order_id)
        if order:
            order.status = OrderStatus.UNLOCKED
            order.unlocked_at = datetime.now().isoformat()
            return self.update_order(order)
        return False

    def get_pending_orders(self) -> List[CloudOrder]:
        """Get all pending/paid orders (for admin dashboard)."""
        if not self.is_connected:
            return []

        try:
            result = self.supabase.table(self.TABLE_NAME).select("*").in_(
                "status", ["pending", "paid"]
            ).order("created_at", desc=True).execute()
            return [CloudOrder.from_dict(row) for row in result.data]
        except Exception as e:
            print(f"ERROR: Failed to get pending orders: {e}")
            return []

    def get_all_orders(self, limit: int = 50) -> List[CloudOrder]:
        """Get all orders (for admin dashboard)."""
        if not self.is_connected:
            return []

        try:
            result = self.supabase.table(self.TABLE_NAME).select("*").order(
                "created_at", desc=True
            ).limit(limit).execute()
            return [CloudOrder.from_dict(row) for row in result.data]
        except Exception as e:
            print(f"ERROR: Failed to get orders: {e}")
            return []

    def check_verification_status(self, order_id: str) -> Optional[str]:
        """Check if order has been verified by admin (for polling)."""
        order = self.get_order(order_id)
        if order:
            return order.status.value
        return None
