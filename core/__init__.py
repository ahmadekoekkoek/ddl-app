"""
Core Infrastructure Module
Provides logging, defensive programming, error handling, and memory optimization.
"""

from .logging_config import (
    setup_logging, get_logger, LogContext,
    log_performance, log_audit, mask_sensitive
)
from .defensive import (
    CircuitBreaker, CircuitState,
    validate_entity_lines, validate_dataframe,
    sanitize_pii, sanitize_path, ResourceManager
)
from .errors import (
    ErrorClassifier, ErrorCategory, RecoveryStrategy,
    ScrapingError, APIError, DataError, ReportError,
    NetworkError, AuthenticationError, RateLimitError,
    ErrorReport, graceful_degradation
)
from .memory import (
    MemoryMonitor, ChunkProcessor, DataOptimizer,
    memory_efficient, cleanup_resources
)

__all__ = [
    # Logging
    'setup_logging', 'get_logger', 'LogContext',
    'log_performance', 'log_audit', 'mask_sensitive',

    # Defensive
    'CircuitBreaker', 'CircuitState',
    'validate_entity_lines', 'validate_dataframe',
    'sanitize_pii', 'sanitize_path', 'ResourceManager',

    # Errors
    'ErrorClassifier', 'ErrorCategory', 'RecoveryStrategy',
    'ScrapingError', 'APIError', 'DataError', 'ReportError',
    'NetworkError', 'AuthenticationError', 'RateLimitError',
    'ErrorReport', 'graceful_degradation',

    # Memory
    'MemoryMonitor', 'ChunkProcessor', 'DataOptimizer',
    'memory_efficient', 'cleanup_resources',
]
