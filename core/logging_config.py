"""
Structured Logging System
Comprehensive logging with context, performance tracking, and audit trails.
"""

import os
import sys
import json
import time
import logging
import threading
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from functools import wraps
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
import uuid


class ColoredFormatter(logging.Formatter):
    """Colored console output for development."""

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # Add color to level name
        record.levelname = f"{color}{record.levelname}{reset}"

        # Format timestamp
        record.asctime = datetime.fromtimestamp(record.created).strftime('%H:%M:%S.%f')[:-3]

        return super().format(record)


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging in production."""

    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add extra fields from LogContext
        if hasattr(record, 'context'):
            log_entry['context'] = record.context

        # Add exception info
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False, default=str)


@dataclass
class LogContext:
    """Thread-local logging context for tracking operations."""

    transaction_id: str = ""
    session_id: str = ""
    user_id: str = ""
    component: str = ""
    stage: str = ""
    entity_count: int = 0
    family_id: str = ""

    _local = threading.local()

    @classmethod
    def get_current(cls) -> 'LogContext':
        """Get current thread's context."""
        if not hasattr(cls._local, 'context'):
            cls._local.context = cls()
        return cls._local.context

    @classmethod
    def set(cls, **kwargs):
        """Set context values."""
        ctx = cls.get_current()
        for key, value in kwargs.items():
            if hasattr(ctx, key):
                setattr(ctx, key, value)

    @classmethod
    def clear(cls):
        """Clear context."""
        cls._local.context = cls()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding empty values."""
        return {k: v for k, v in asdict(self).items() if v}


class ContextFilter(logging.Filter):
    """Adds context information to log records."""

    def filter(self, record):
        ctx = LogContext.get_current()
        record.context = ctx.to_dict()

        # Add context to message for console
        if ctx.component:
            record.component = f"[{ctx.component}]"
        else:
            record.component = ""

        return True


class PerformanceLogger:
    """Tracks and logs performance metrics."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._timers: Dict[str, float] = {}
        self._metrics: Dict[str, list] = {}

    def start_timer(self, operation: str):
        """Start timing an operation."""
        self._timers[operation] = time.perf_counter()

    def stop_timer(self, operation: str) -> float:
        """Stop timer and log duration."""
        if operation not in self._timers:
            return 0.0

        duration = (time.perf_counter() - self._timers[operation]) * 1000
        del self._timers[operation]

        # Track metrics
        if operation not in self._metrics:
            self._metrics[operation] = []
        self._metrics[operation].append(duration)

        self.logger.debug(f"Performance: {operation} took {duration:.2f}ms")
        return duration

    @contextmanager
    def timed_operation(self, operation: str):
        """Context manager for timing operations."""
        self.start_timer(operation)
        try:
            yield
        finally:
            self.stop_timer(operation)

    def get_stats(self, operation: str) -> Dict[str, float]:
        """Get statistics for an operation."""
        if operation not in self._metrics or not self._metrics[operation]:
            return {}

        values = self._metrics[operation]
        return {
            'count': len(values),
            'total_ms': sum(values),
            'avg_ms': sum(values) / len(values),
            'min_ms': min(values),
            'max_ms': max(values)
        }

    def log_summary(self):
        """Log summary of all performance metrics."""
        for operation in self._metrics:
            stats = self.get_stats(operation)
            if stats:
                self.logger.info(
                    f"Performance Summary - {operation}: "
                    f"count={stats['count']}, avg={stats['avg_ms']:.2f}ms, "
                    f"total={stats['total_ms']:.2f}ms"
                )


def mask_sensitive(text: str, mask_char: str = '*') -> str:
    """Mask sensitive data like NIK, tokens, etc."""
    if not text or len(text) < 8:
        return text

    text = str(text)

    # NIK pattern (16 digits)
    if len(text) == 16 and text.isdigit():
        return text[:4] + mask_char * 8 + text[-4:]

    # Token/Bearer patterns
    if len(text) > 20:
        return text[:8] + mask_char * (len(text) - 12) + text[-4:]

    # Generic masking
    visible = max(2, len(text) // 4)
    return text[:visible] + mask_char * (len(text) - visible * 2) + text[-visible:]


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_format: bool = False,
    component: str = "scraper"
) -> logging.Logger:
    """
    Set up structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for file logging
        json_format: Use JSON format for production
        component: Component name for context

    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger('dtsen')
    logger.setLevel(getattr(logging, level.upper()))
    logger.handlers.clear()

    # Add context filter
    context_filter = ContextFilter()
    logger.addFilter(context_filter)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)

    if json_format:
        console.setFormatter(JSONFormatter())
    else:
        console.setFormatter(ColoredFormatter(
            '%(asctime)s %(levelname)s %(component)s %(message)s'
        ))

    logger.addHandler(console)

    # File handler (JSON format for structured logging)
    if log_file:
        os.makedirs(os.path.dirname(log_file) or '.', exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)

    # Set default context
    LogContext.set(
        component=component,
        transaction_id=str(uuid.uuid4())[:8]
    )

    logger.info(f"Logging initialized: level={level}, json={json_format}")

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(f'dtsen.{name}' if name else 'dtsen')


def log_performance(operation: str):
    """Decorator to log function performance."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger('performance')
            start = time.perf_counter()

            try:
                result = func(*args, **kwargs)
                duration = (time.perf_counter() - start) * 1000
                logger.debug(f"{operation}: {func.__name__} completed in {duration:.2f}ms")
                return result
            except Exception as e:
                duration = (time.perf_counter() - start) * 1000
                logger.error(f"{operation}: {func.__name__} failed after {duration:.2f}ms - {e}")
                raise

        return wrapper
    return decorator


def log_audit(event_type: str, details: Dict[str, Any] = None):
    """Log an audit event."""
    logger = get_logger('audit')
    ctx = LogContext.get_current()

    audit_entry = {
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'transaction_id': ctx.transaction_id,
        'user_id': ctx.user_id,
        'details': details or {}
    }

    logger.info(f"AUDIT: {event_type}", extra={'audit': audit_entry})


# Convenience functions for different log levels with context
def log_api_call(url: str, status: int, duration_ms: float):
    """Log API call details."""
    logger = get_logger('api')
    logger.debug(f"API: {url} -> {status} ({duration_ms:.2f}ms)")


def log_data_operation(operation: str, records: int, duration_ms: float = None):
    """Log data processing operation."""
    logger = get_logger('data')
    msg = f"Data: {operation} - {records} records"
    if duration_ms:
        msg += f" ({duration_ms:.2f}ms)"
    logger.info(msg)


def log_memory_usage():
    """Log current memory usage."""
    try:
        import psutil
        process = psutil.Process()
        mem_mb = process.memory_info().rss / (1024 * 1024)
        logger = get_logger('memory')
        logger.debug(f"Memory: {mem_mb:.1f} MB")
    except ImportError:
        pass
