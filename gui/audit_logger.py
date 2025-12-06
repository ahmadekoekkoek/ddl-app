"""
Audit Logging System
Comprehensive logging with checksums and encrypted storage for compliance.
"""

import os
import json
import time
import hashlib
import gzip
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime
from enum import Enum
import threading

from cryptography.fernet import Fernet

from PySide6.QtCore import QObject, Signal


class AuditEventType(Enum):
    """Types of audit events."""
    # User actions
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    CONFIG_CHANGE = "config_change"
    PROFILE_LOAD = "profile_load"
    PROFILE_SAVE = "profile_save"

    # Scraping operations
    SCRAPE_START = "scrape_start"
    SCRAPE_COMPLETE = "scrape_complete"
    SCRAPE_ERROR = "scrape_error"
    SCRAPE_CANCELLED = "scrape_cancelled"

    # Payment operations
    PAYMENT_INITIATED = "payment_initiated"
    PAYMENT_COMPLETED = "payment_completed"
    PAYMENT_FAILED = "payment_failed"

    # System events
    APP_START = "app_start"
    APP_EXIT = "app_exit"
    ERROR = "error"
    WARNING = "warning"

    # Security events
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    ENCRYPTION_APPLIED = "encryption_applied"
    DATA_EXPORT = "data_export"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEntry:
    """Represents a single audit log entry."""
    timestamp: float
    event_type: str
    severity: str
    message: str
    details: Dict[str, Any]
    checksum: str = ""
    session_id: str = ""
    user_id: str = ""
    source: str = ""

    def __post_init__(self):
        if not self.checksum:
            self.checksum = self._calculate_checksum()

    def _calculate_checksum(self) -> str:
        """Calculate SHA-256 checksum for integrity verification."""
        data = f"{self.timestamp}|{self.event_type}|{self.severity}|{self.message}|{json.dumps(self.details, sort_keys=True)}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def verify_integrity(self) -> bool:
        """Verify the entry hasn't been tampered with."""
        expected = self._calculate_checksum()
        return self.checksum == expected

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuditEntry':
        """Create from dictionary."""
        return cls(**data)


class LogEncryption:
    """Handles encryption of audit logs."""

    def __init__(self, key: bytes = None):
        if key:
            self.fernet = Fernet(key)
        else:
            self.key = Fernet.generate_key()
            self.fernet = Fernet(self.key)

    def encrypt(self, data: str) -> bytes:
        """Encrypt log data."""
        return self.fernet.encrypt(data.encode())

    def decrypt(self, encrypted: bytes) -> str:
        """Decrypt log data."""
        return self.fernet.decrypt(encrypted).decode()

    def get_key(self) -> bytes:
        """Get encryption key for storage."""
        return self.key


class AuditLogger(QObject):
    """Main audit logging system."""

    entry_logged = Signal(AuditEntry)
    log_rotated = Signal(str)  # new log file path

    LOGS_DIR = "audit_logs"
    MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
    MAX_LOG_AGE_DAYS = 90

    def __init__(self, base_path: str = None, encrypted: bool = True):
        super().__init__()

        self.base_path = Path(base_path or ".").resolve()
        self.logs_path = self.base_path / self.LOGS_DIR
        self.logs_path.mkdir(exist_ok=True)

        self.encrypted = encrypted
        self._encryption: Optional[LogEncryption] = None

        if encrypted:
            self._setup_encryption()

        self._session_id = self._generate_session_id()
        self._user_id = ""
        self._current_log_file: Optional[Path] = None
        self._entries_buffer: List[AuditEntry] = []
        self._buffer_lock = threading.Lock()
        self._flush_interval = 5  # seconds

        self._init_log_file()
        self._start_flush_timer()

    def _generate_session_id(self) -> str:
        """Generate unique session ID."""
        return hashlib.sha256(f"{time.time()}{os.getpid()}".encode()).hexdigest()[:12]

    def _setup_encryption(self):
        """Set up log encryption."""
        key_file = self.logs_path / ".audit_key"

        if key_file.exists():
            with open(key_file, 'rb') as f:
                key = f.read()
            self._encryption = LogEncryption(key)
        else:
            self._encryption = LogEncryption()
            with open(key_file, 'wb') as f:
                f.write(self._encryption.get_key())

    def _init_log_file(self):
        """Initialize current log file."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        ext = ".log.enc" if self.encrypted else ".log"
        self._current_log_file = self.logs_path / f"audit_{date_str}{ext}"

    def _start_flush_timer(self):
        """Start background flush timer."""
        def flush_loop():
            while True:
                time.sleep(self._flush_interval)
                self.flush()

        thread = threading.Thread(target=flush_loop, daemon=True)
        thread.start()

    def set_user(self, user_id: str):
        """Set current user for audit entries."""
        self._user_id = user_id

    def log(self, event_type: AuditEventType, message: str,
            severity: AuditSeverity = AuditSeverity.INFO,
            details: Dict[str, Any] = None, source: str = "") -> AuditEntry:
        """Log an audit event."""
        entry = AuditEntry(
            timestamp=time.time(),
            event_type=event_type.value,
            severity=severity.value,
            message=message,
            details=details or {},
            session_id=self._session_id,
            user_id=self._user_id,
            source=source or "gui"
        )

        with self._buffer_lock:
            self._entries_buffer.append(entry)

        self.entry_logged.emit(entry)

        # Immediate flush for critical events
        if severity in [AuditSeverity.ERROR, AuditSeverity.CRITICAL]:
            self.flush()

        return entry

    def log_config_change(self, field: str, old_value: Any, new_value: Any):
        """Log configuration changes."""
        # Mask sensitive data
        if 'token' in field.lower() or 'password' in field.lower():
            old_value = "***MASKED***"
            new_value = "***MASKED***"

        self.log(
            AuditEventType.CONFIG_CHANGE,
            f"Configuration '{field}' changed",
            details={"field": field, "old": str(old_value), "new": str(new_value)}
        )

    def log_scrape_operation(self, operation: str, entity_count: int = 0,
                            success: bool = True, error: str = None):
        """Log scraping operations."""
        event_type = AuditEventType.SCRAPE_COMPLETE if success else AuditEventType.SCRAPE_ERROR
        severity = AuditSeverity.INFO if success else AuditSeverity.ERROR

        self.log(
            event_type,
            f"Scrape {operation}: {'success' if success else 'failed'}",
            severity=severity,
            details={
                "operation": operation,
                "entity_count": entity_count,
                "success": success,
                "error": error
            }
        )

    def log_payment(self, status: str, amount: float = 0, transaction_id: str = None):
        """Log payment events."""
        event_map = {
            "initiated": AuditEventType.PAYMENT_INITIATED,
            "completed": AuditEventType.PAYMENT_COMPLETED,
            "failed": AuditEventType.PAYMENT_FAILED,
        }

        self.log(
            event_map.get(status, AuditEventType.PAYMENT_INITIATED),
            f"Payment {status}",
            details={
                "amount": amount,
                "transaction_id": transaction_id or "",
                "status": status
            }
        )

    def log_error(self, error: Exception, context: str = ""):
        """Log application errors."""
        self.log(
            AuditEventType.ERROR,
            str(error),
            severity=AuditSeverity.ERROR,
            details={
                "error_type": type(error).__name__,
                "context": context,
            }
        )

    def flush(self):
        """Flush buffered entries to disk."""
        with self._buffer_lock:
            if not self._entries_buffer:
                return

            entries = self._entries_buffer.copy()
            self._entries_buffer.clear()

        self._check_rotation()

        # Write entries
        lines = [json.dumps(e.to_dict()) for e in entries]
        data = "\n".join(lines) + "\n"

        if self.encrypted and self._encryption:
            encrypted = self._encryption.encrypt(data)
            with open(self._current_log_file, 'ab') as f:
                f.write(encrypted + b'\n---ENTRY---\n')
        else:
            with open(self._current_log_file, 'a', encoding='utf-8') as f:
                f.write(data)

    def _check_rotation(self):
        """Check if log rotation is needed."""
        if not self._current_log_file.exists():
            return

        if self._current_log_file.stat().st_size > self.MAX_LOG_SIZE:
            self._rotate_log()

    def _rotate_log(self):
        """Rotate current log file."""
        old_file = self._current_log_file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Compress old log
        compressed = old_file.with_suffix(old_file.suffix + '.gz')
        with open(old_file, 'rb') as f_in:
            with gzip.open(compressed, 'wb') as f_out:
                f_out.write(f_in.read())

        old_file.unlink()

        # Create new log file
        self._init_log_file()
        self.log_rotated.emit(str(compressed))

    def read_entries(self, start_date: datetime = None, end_date: datetime = None,
                    event_types: List[AuditEventType] = None,
                    limit: int = 1000) -> List[AuditEntry]:
        """Read audit entries with filtering."""
        entries = []

        for log_file in sorted(self.logs_path.glob("audit_*")):
            if log_file.suffix == '.gz':
                continue

            try:
                if self.encrypted and log_file.suffix == '.enc':
                    content = self._read_encrypted_log(log_file)
                else:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        content = f.read()

                for line in content.strip().split('\n'):
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        entry = AuditEntry.from_dict(data)

                        # Apply filters
                        if start_date and entry.timestamp < start_date.timestamp():
                            continue
                        if end_date and entry.timestamp > end_date.timestamp():
                            continue
                        if event_types and entry.event_type not in [e.value for e in event_types]:
                            continue

                        entries.append(entry)

                        if len(entries) >= limit:
                            break
                    except (json.JSONDecodeError, TypeError):
                        continue

            except Exception as e:
                print(f"Error reading log file {log_file}: {e}")

        return entries

    def _read_encrypted_log(self, log_file: Path) -> str:
        """Read and decrypt a log file."""
        if not self._encryption:
            return ""

        with open(log_file, 'rb') as f:
            content = f.read()

        # Split by entry separator
        parts = content.split(b'\n---ENTRY---\n')
        decrypted_parts = []

        for part in parts:
            if part.strip():
                try:
                    decrypted = self._encryption.decrypt(part)
                    decrypted_parts.append(decrypted)
                except Exception:
                    pass

        return "\n".join(decrypted_parts)

    def verify_log_integrity(self, log_file: Path = None) -> Dict[str, Any]:
        """Verify integrity of log entries."""
        files_to_check = [log_file] if log_file else list(self.logs_path.glob("audit_*.log*"))

        results = {
            "total_entries": 0,
            "valid_entries": 0,
            "tampered_entries": 0,
            "errors": []
        }

        for file in files_to_check:
            if file.suffix == '.gz':
                continue

            entries = self.read_entries(limit=10000)
            for entry in entries:
                results["total_entries"] += 1
                if entry.verify_integrity():
                    results["valid_entries"] += 1
                else:
                    results["tampered_entries"] += 1
                    results["errors"].append({
                        "timestamp": entry.timestamp,
                        "event_type": entry.event_type,
                        "message": "Checksum verification failed"
                    })

        return results

    def export_logs(self, output_path: str, start_date: datetime = None,
                   end_date: datetime = None, format: str = "json") -> str:
        """Export logs to a file."""
        entries = self.read_entries(start_date=start_date, end_date=end_date)

        if format == "json":
            data = json.dumps([e.to_dict() for e in entries], indent=2)
            ext = ".json"
        else:
            # CSV format
            headers = ["timestamp", "event_type", "severity", "message", "session_id", "checksum"]
            lines = [",".join(headers)]
            for e in entries:
                row = [
                    datetime.fromtimestamp(e.timestamp).isoformat(),
                    e.event_type,
                    e.severity,
                    f'"{e.message}"',
                    e.session_id,
                    e.checksum
                ]
                lines.append(",".join(row))
            data = "\n".join(lines)
            ext = ".csv"

        output_file = Path(output_path).with_suffix(ext)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(data)

        self.log(
            AuditEventType.DATA_EXPORT,
            f"Audit logs exported to {output_file}",
            details={"entry_count": len(entries), "format": format}
        )

        return str(output_file)

    def cleanup_old_logs(self) -> int:
        """Remove logs older than MAX_LOG_AGE_DAYS."""
        cutoff = time.time() - (self.MAX_LOG_AGE_DAYS * 24 * 60 * 60)
        removed = 0

        for log_file in self.logs_path.glob("audit_*"):
            if log_file.stat().st_mtime < cutoff:
                log_file.unlink()
                removed += 1

        return removed


# Singleton instance
_audit_logger: Optional[AuditLogger] = None

def get_audit_logger(base_path: str = None, encrypted: bool = True) -> AuditLogger:
    """Get the global audit logger instance."""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger(base_path, encrypted)
    return _audit_logger
