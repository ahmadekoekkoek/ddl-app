"""
Defensive Programming Utilities
Circuit breaker, input validation, data sanitization, and resource management.
"""

import os
import re
import time
import threading
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from functools import wraps
from enum import Enum
from contextlib import contextmanager
import pandas as pd

from .logging_config import get_logger, mask_sensitive


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreaker:
    """
    Circuit breaker pattern for API calls.

    After 'failure_threshold' consecutive failures, the circuit opens
    and rejects all calls for 'recovery_timeout' seconds. After that,
    it enters half-open state to test if the service recovered.
    """

    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 300  # 5 minutes

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def __post_init__(self):
        self._logger = get_logger('circuit_breaker')

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._logger.info(f"Circuit '{self.name}' entering HALF_OPEN state")
            return self._state

    def record_success(self):
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._logger.info(f"Circuit '{self.name}' CLOSED - service recovered")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def record_failure(self, error: Exception = None):
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._logger.warning(f"Circuit '{self.name}' OPENED - half-open test failed")

            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._logger.warning(
                        f"Circuit '{self.name}' OPENED after {self._failure_count} failures"
                    )

    def is_available(self) -> bool:
        """Check if calls are allowed."""
        state = self.state
        return state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    def __call__(self, func: Callable) -> Callable:
        """Use as decorator."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not self.is_available():
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN. Retry after {self.recovery_timeout}s"
                )

            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception as e:
                self.record_failure(e)
                raise

        return wrapper


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


# Validation Decorators

def validate_entity_lines(min_length: int = 1, max_length: int = 10000):
    """Decorator to validate entity lines input."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find entity_lines in args or kwargs
            entity_lines = kwargs.get('entity_lines')
            if entity_lines is None and len(args) > 1:
                entity_lines = args[1]  # Usually second arg after self

            if entity_lines is None:
                raise ValueError("entity_lines parameter is required")

            if not isinstance(entity_lines, (list, tuple)):
                raise TypeError(f"entity_lines must be a list, got {type(entity_lines)}")

            if len(entity_lines) < min_length:
                raise ValueError(f"entity_lines must have at least {min_length} items")

            if len(entity_lines) > max_length:
                raise ValueError(f"entity_lines exceeds maximum length of {max_length}")

            # Validate each entry is non-empty string
            for i, entry in enumerate(entity_lines):
                if not isinstance(entry, str) or not entry.strip():
                    raise ValueError(f"entity_lines[{i}] must be a non-empty string")

            return func(*args, **kwargs)
        return wrapper
    return decorator


def validate_dataframe(required_columns: List[str] = None, min_rows: int = 0):
    """Decorator to validate DataFrame inputs."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find DataFrame in args or kwargs
            df = None
            for arg in args:
                if isinstance(arg, pd.DataFrame):
                    df = arg
                    break

            if df is None:
                for key, val in kwargs.items():
                    if isinstance(val, pd.DataFrame):
                        df = val
                        break

            if df is not None:
                if required_columns:
                    missing = set(required_columns) - set(df.columns)
                    if missing:
                        raise ValueError(f"DataFrame missing required columns: {missing}")

                if len(df) < min_rows:
                    raise ValueError(f"DataFrame must have at least {min_rows} rows")

            return func(*args, **kwargs)
        return wrapper
    return decorator


# Data Sanitization

class PIISanitizer:
    """Sanitizes personally identifiable information."""

    # Patterns for sensitive data
    NIK_PATTERN = re.compile(r'\b\d{16}\b')
    PHONE_PATTERN = re.compile(r'\b(08\d{8,12}|62\d{9,13})\b')
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

    @classmethod
    def sanitize(cls, text: str, replacement: str = '***MASKED***') -> str:
        """Sanitize all PII patterns in text."""
        if not text:
            return text

        text = str(text)

        # Mask NIK (keep first 4 and last 4)
        text = cls.NIK_PATTERN.sub(lambda m: m.group()[:4] + '*' * 8 + m.group()[-4:], text)

        # Mask phone numbers
        text = cls.PHONE_PATTERN.sub(replacement, text)

        # Mask email addresses
        text = cls.EMAIL_PATTERN.sub(lambda m: m.group().split('@')[0][:2] + '***@***', text)

        return text

    @classmethod
    def sanitize_dict(cls, data: Dict, sensitive_keys: Set[str] = None) -> Dict:
        """Sanitize dictionary values."""
        sensitive_keys = sensitive_keys or {'nik', 'NIK', 'token', 'password', 'secret', 'bearer'}

        sanitized = {}
        for key, value in data.items():
            if any(s in key.lower() for s in sensitive_keys):
                sanitized[key] = mask_sensitive(str(value)) if value else value
            elif isinstance(value, str):
                sanitized[key] = cls.sanitize(value)
            elif isinstance(value, dict):
                sanitized[key] = cls.sanitize_dict(value, sensitive_keys)
            else:
                sanitized[key] = value

        return sanitized

    @classmethod
    def sanitize_html(cls, text: str) -> str:
        """Sanitize text for HTML output to prevent injection."""
        if not text:
            return ""
        # Basic escaping
        text = str(text)
        return (text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace('"', "&quot;")
                   .replace("'", "&#x27;"))

    @classmethod
    def sanitize_pdf_text(cls, text: str) -> str:
        """Sanitize text for PDF output."""
        if not text:
            return ""
        # Remove control characters that might break PDF generation
        text = str(text)
        # Remove null bytes and other non-printable chars (except newlines/tabs)
        return "".join(ch for ch in text if ch == '\n' or ch == '\t' or ch >= ' ')


def sanitize_pii(text: str) -> str:
    """Convenience function for PII sanitization."""
    return PIISanitizer.sanitize(text)


def sanitize_path(path: str, base_dir: str = None) -> str:
    """
    Sanitize file path to prevent directory traversal attacks.

    Args:
        path: User-provided path
        base_dir: Allowed base directory

    Returns:
        Sanitized absolute path

    Raises:
        ValueError: If path attempts directory traversal
    """
    # Remove null bytes
    path = path.replace('\x00', '')

    # Normalize path
    normalized = os.path.normpath(path)

    # Check for traversal attempts
    if '..' in normalized:
        raise ValueError(f"Directory traversal detected in path: {path}")

    # If base_dir specified, ensure path is within it
    if base_dir:
        base = os.path.abspath(base_dir)
        full_path = os.path.abspath(os.path.join(base, normalized))

        if not full_path.startswith(base):
            raise ValueError(f"Path escapes base directory: {path}")

        return full_path

    return os.path.abspath(normalized)


# Resource Management

class ResourceManager:
    """Context manager for managing multiple resources."""

    def __init__(self):
        self._resources: List[Any] = []
        self._cleanup_funcs: List[Callable] = []
        self._logger = get_logger('resources')

    def register(self, resource: Any, cleanup_func: Callable = None):
        """Register a resource for cleanup."""
        self._resources.append(resource)

        if cleanup_func:
            self._cleanup_funcs.append((resource, cleanup_func))
        elif hasattr(resource, 'close'):
            self._cleanup_funcs.append((resource, resource.close))
        elif hasattr(resource, '__exit__'):
            self._cleanup_funcs.append((resource, lambda r: r.__exit__(None, None, None)))

        return resource

    def cleanup(self):
        """Clean up all registered resources."""
        errors = []

        for resource, cleanup_func in reversed(self._cleanup_funcs):
            try:
                cleanup_func()
            except Exception as e:
                errors.append((resource, e))
                self._logger.warning(f"Resource cleanup failed: {e}")

        self._resources.clear()
        self._cleanup_funcs.clear()

        if errors:
            self._logger.warning(f"{len(errors)} resources failed to clean up properly")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False


@contextmanager
def managed_resources():
    """Context manager for automatic resource cleanup."""
    manager = ResourceManager()
    try:
        yield manager
    finally:
        manager.cleanup()


# Input Validators

def validate_bearer_token(token: str) -> bool:
    """Validate bearer token format."""
    if not token:
        return False

    # Should start with "Bearer " or be a raw token
    if token.startswith("Bearer "):
        token = token[7:]

    # Basic length check
    if len(token) < 20:
        return False

    # Should be alphanumeric with some special chars
    if not re.match(r'^[A-Za-z0-9._\-]+$', token):
        return False

    return True


def validate_id_keluarga(id_keluarga: str) -> bool:
    """Validate family ID format."""
    if not id_keluarga:
        return False

    # Should be numeric or UUID-like
    if id_keluarga.isdigit():
        return len(id_keluarga) >= 8

    # UUID format
    uuid_pattern = re.compile(r'^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$', re.I)
    return bool(uuid_pattern.match(id_keluarga))


class InputValidator:
    """Centralized input validation."""

    @staticmethod
    def validate_config(config: Dict) -> List[str]:
        """Validate configuration dictionary."""
        errors = []

        if 'bearer_token' not in config:
            errors.append("Missing bearer_token")
        elif not validate_bearer_token(config['bearer_token']):
            errors.append("Invalid bearer_token format")

        if 'entity_lines' not in config:
            errors.append("Missing entity_lines")
        elif not config['entity_lines']:
            errors.append("entity_lines is empty")

        return errors

    @staticmethod
    def validate_output_path(path: str) -> List[str]:
        """Validate output path."""
        errors = []

        try:
            sanitized = sanitize_path(path)

            # Check if parent directory exists or can be created
            parent = os.path.dirname(sanitized)
            if parent and not os.path.exists(parent):
                try:
                    os.makedirs(parent, exist_ok=True)
                except OSError as e:
                    errors.append(f"Cannot create output directory: {e}")

        except ValueError as e:
            errors.append(str(e))

        return errors
