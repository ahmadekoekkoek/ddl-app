"""
Error Classification and Recovery System
Categorizes errors and provides recovery strategies.
"""

import os
import json
import traceback
from typing import Dict, List, Any, Optional, Callable, Type
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from functools import wraps

from .logging_config import get_logger


class ErrorCategory(Enum):
    """Error classification categories."""
    RECOVERABLE = "recoverable"    # Can retry with same params
    RETRYABLE = "retryable"        # Temporary, retry after delay
    FATAL = "fatal"                # Cannot recover, stop processing
    PARTIAL = "partial"            # Some data lost, continue with rest


class RecoveryStrategy(Enum):
    """Available recovery strategies."""
    RETRY = "retry"                # Retry the operation
    FALLBACK = "fallback"          # Use fallback data/method
    SKIP = "skip"                  # Skip and continue
    ABORT = "abort"                # Stop processing
    CACHE = "cache"                # Use cached data
    DEGRADE = "degrade"            # Use degraded output


# Custom Exception Classes

class BaseError(Exception):
    """Base class for all application errors."""

    category: ErrorCategory = ErrorCategory.FATAL
    recovery: RecoveryStrategy = RecoveryStrategy.ABORT

    def __init__(self, message: str, details: Dict = None, cause: Exception = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict:
        return {
            'type': self.__class__.__name__,
            'message': self.message,
            'category': self.category.value,
            'recovery': self.recovery.value,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'cause': str(self.cause) if self.cause else None
        }


class APIError(BaseError):
    """API call errors."""
    category = ErrorCategory.RETRYABLE
    recovery = RecoveryStrategy.RETRY

    def __init__(self, message: str, status_code: int = None, **kwargs):
        super().__init__(message, **kwargs)
        self.status_code = status_code
        self.details['status_code'] = status_code

        # Classify based on status code
        if status_code == 401:
            self.category = ErrorCategory.FATAL
            self.recovery = RecoveryStrategy.ABORT
        elif status_code == 429:
            self.category = ErrorCategory.RETRYABLE
            self.recovery = RecoveryStrategy.RETRY
        elif status_code and status_code >= 500:
            self.category = ErrorCategory.RETRYABLE
            self.recovery = RecoveryStrategy.RETRY


class NetworkError(APIError):
    """Network connectivity errors."""
    category = ErrorCategory.RETRYABLE
    recovery = RecoveryStrategy.RETRY


class AuthenticationError(APIError):
    """Authentication/authorization errors."""
    category = ErrorCategory.FATAL
    recovery = RecoveryStrategy.ABORT


class RateLimitError(APIError):
    """Rate limiting errors."""
    category = ErrorCategory.RETRYABLE
    recovery = RecoveryStrategy.RETRY

    def __init__(self, message: str, retry_after: float = None, **kwargs):
        super().__init__(message, status_code=429, **kwargs)
        self.retry_after = retry_after
        self.details['retry_after'] = retry_after


class DataError(BaseError):
    """Data processing errors."""
    category = ErrorCategory.PARTIAL
    recovery = RecoveryStrategy.SKIP


class DecryptionError(DataError):
    """Decryption failures."""
    category = ErrorCategory.PARTIAL
    recovery = RecoveryStrategy.SKIP


class ValidationError(DataError):
    """Data validation errors."""
    category = ErrorCategory.PARTIAL
    recovery = RecoveryStrategy.SKIP


class ScrapingError(BaseError):
    """Scraping operation errors."""
    category = ErrorCategory.PARTIAL
    recovery = RecoveryStrategy.FALLBACK


class ReportError(BaseError):
    """Report generation errors."""
    category = ErrorCategory.RECOVERABLE
    recovery = RecoveryStrategy.FALLBACK


class ExcelError(ReportError):
    """Excel generation errors."""
    recovery = RecoveryStrategy.FALLBACK  # Fallback to CSV


class PDFError(ReportError):
    """PDF generation errors."""
    recovery = RecoveryStrategy.DEGRADE  # Simplified text report


class ChartError(ReportError):
    """Chart generation errors."""
    recovery = RecoveryStrategy.FALLBACK  # Use data table instead


# Error Classification

class ErrorClassifier:
    """Classifies exceptions into categories."""

    # Mapping of exception types to categories
    EXCEPTION_MAP = {
        ConnectionError: (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        TimeoutError: (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        PermissionError: (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        FileNotFoundError: (ErrorCategory.RECOVERABLE, RecoveryStrategy.FALLBACK),
        MemoryError: (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        KeyError: (ErrorCategory.PARTIAL, RecoveryStrategy.SKIP),
        ValueError: (ErrorCategory.PARTIAL, RecoveryStrategy.SKIP),
    }

    # Keywords that indicate specific categories
    KEYWORD_MAP = {
        'timeout': (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        'connection': (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        'unauthorized': (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        '401': (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        'rate limit': (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        '429': (ErrorCategory.RETRYABLE, RecoveryStrategy.RETRY),
        'memory': (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        'corrupt': (ErrorCategory.FATAL, RecoveryStrategy.ABORT),
        'decrypt': (ErrorCategory.PARTIAL, RecoveryStrategy.SKIP),
        'parse': (ErrorCategory.PARTIAL, RecoveryStrategy.SKIP),
    }

    @classmethod
    def classify(cls, error: Exception) -> tuple:
        """
        Classify an exception.

        Returns:
            Tuple of (ErrorCategory, RecoveryStrategy)
        """
        # Check if it's our custom error
        if isinstance(error, BaseError):
            return (error.category, error.recovery)

        # Check exception type mapping
        for exc_type, classification in cls.EXCEPTION_MAP.items():
            if isinstance(error, exc_type):
                return classification

        # Check error message for keywords
        error_msg = str(error).lower()
        for keyword, classification in cls.KEYWORD_MAP.items():
            if keyword in error_msg:
                return classification

        # Default: treat as recoverable
        return (ErrorCategory.RECOVERABLE, RecoveryStrategy.FALLBACK)

    @classmethod
    def is_retryable(cls, error: Exception) -> bool:
        """Check if error is retryable."""
        category, _ = cls.classify(error)
        return category == ErrorCategory.RETRYABLE

    @classmethod
    def is_fatal(cls, error: Exception) -> bool:
        """Check if error is fatal."""
        category, _ = cls.classify(error)
        return category == ErrorCategory.FATAL


# Error Reports

@dataclass
class ErrorReport:
    """Detailed error report for analysis."""

    error_type: str
    message: str
    category: str
    recovery_attempted: str
    recovery_success: bool
    data_lost: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    stack_trace: str = ""
    context: Dict = field(default_factory=dict)
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

    def save(self, path: str):
        """Save error report to file."""
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())

    @classmethod
    def from_exception(
        cls,
        error: Exception,
        recovery_attempted: str = "none",
        recovery_success: bool = False,
        data_lost: List[str] = None,
        context: Dict = None
    ) -> 'ErrorReport':
        """Create report from exception."""
        category, _ = ErrorClassifier.classify(error)

        recommendations = cls._generate_recommendations(error, category)

        return cls(
            error_type=type(error).__name__,
            message=str(error),
            category=category.value,
            recovery_attempted=recovery_attempted,
            recovery_success=recovery_success,
            data_lost=data_lost or [],
            recommendations=recommendations,
            stack_trace=traceback.format_exc(),
            context=context or {}
        )

    @staticmethod
    def _generate_recommendations(error: Exception, category: ErrorCategory) -> List[str]:
        """Generate recommendations based on error."""
        recommendations = []
        error_msg = str(error).lower()

        if category == ErrorCategory.FATAL:
            if '401' in error_msg or 'unauthorized' in error_msg:
                recommendations.append("Check bearer token validity and expiration")
                recommendations.append("Re-authenticate and obtain a new token")
            elif 'memory' in error_msg:
                recommendations.append("Reduce batch size for processing")
                recommendations.append("Close other applications to free memory")

        elif category == ErrorCategory.RETRYABLE:
            recommendations.append("Wait and retry the operation")
            if 'timeout' in error_msg:
                recommendations.append("Check network connectivity")
                recommendations.append("Consider increasing timeout value")
            if '429' in error_msg:
                recommendations.append("Reduce request rate")
                recommendations.append("Implement exponential backoff")

        elif category == ErrorCategory.PARTIAL:
            recommendations.append("Review skipped data for manual processing")
            recommendations.append("Check data format and validation rules")

        return recommendations


# Graceful Degradation

class GracefulDegradation:
    """Manages graceful degradation when components fail."""

    def __init__(self):
        self._fallbacks: Dict[str, Callable] = {}
        self._failed_components: List[str] = []
        self._logger = get_logger('degradation')

    def register_fallback(self, component: str, fallback_func: Callable):
        """Register a fallback function for a component."""
        self._fallbacks[component] = fallback_func

    def mark_failed(self, component: str, error: Exception):
        """Mark a component as failed."""
        self._failed_components.append(component)
        self._logger.warning(f"Component '{component}' failed: {error}")

    def execute_with_fallback(self, component: str, primary_func: Callable, *args, **kwargs):
        """Execute function with fallback on failure."""
        try:
            return primary_func(*args, **kwargs)
        except Exception as e:
            self.mark_failed(component, e)

            if component in self._fallbacks:
                self._logger.info(f"Using fallback for '{component}'")
                try:
                    return self._fallbacks[component](*args, **kwargs)
                except Exception as fallback_error:
                    self._logger.error(f"Fallback for '{component}' also failed: {fallback_error}")
                    raise
            raise

    def get_status(self) -> Dict:
        """Get degradation status."""
        return {
            'failed_components': self._failed_components,
            'is_degraded': len(self._failed_components) > 0,
            'available_fallbacks': list(self._fallbacks.keys())
        }


def graceful_degradation(component: str, fallback: Callable = None):
    """Decorator for graceful degradation."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger('degradation')
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Component '{component}' failed: {e}")

                if fallback:
                    logger.info(f"Using fallback for '{component}'")
                    try:
                        return fallback(*args, **kwargs)
                    except Exception as fallback_error:
                        logger.error(f"Fallback also failed: {fallback_error}")
                        raise
                raise

        return wrapper
    return decorator


# Recovery Handlers

class RecoveryHandler:
    """Handles error recovery attempts."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._logger = get_logger('recovery')

    def with_retry(self, func: Callable, *args, **kwargs):
        """Execute function with retry logic."""
        import time

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e

                if not ErrorClassifier.is_retryable(e):
                    self._logger.error(f"Non-retryable error: {e}")
                    raise

                delay = self.base_delay * (2 ** (attempt - 1))
                self._logger.warning(f"Attempt {attempt}/{self.max_retries} failed: {e}")
                self._logger.info(f"Retrying in {delay:.1f}s...")
                time.sleep(delay)

        self._logger.error(f"All {self.max_retries} attempts failed")
        raise last_error

    def with_fallback(
        self,
        primary: Callable,
        fallback: Callable,
        *args,
        **kwargs
    ):
        """Execute with fallback on failure."""
        try:
            return primary(*args, **kwargs)
        except Exception as e:
            self._logger.warning(f"Primary function failed: {e}")
            self._logger.info("Attempting fallback...")

            try:
                return fallback(*args, **kwargs)
            except Exception as fallback_error:
                self._logger.error(f"Fallback also failed: {fallback_error}")
                raise
