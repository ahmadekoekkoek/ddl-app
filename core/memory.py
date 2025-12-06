"""
Memory Optimization Utilities
Chunk processing, memory monitoring, and data structure optimization.
"""

import os
import gc
import weakref
import tempfile
from typing import Dict, List, Any, Optional, Callable, Iterator, TypeVar, Generic
from dataclasses import dataclass
from functools import wraps, lru_cache
from contextlib import contextmanager

import pandas as pd
import numpy as np

from .logging_config import get_logger

T = TypeVar('T')


class MemoryMonitor:
    """Monitors and reports memory usage."""

    def __init__(self, warning_threshold_mb: float = None):
        self._logger = get_logger('memory')
        self._baseline_mb: float = 0.0
        self._peak_mb: float = 0.0
        self._warning_threshold = warning_threshold_mb or 300  # Default 300MB
        self._samples: List[float] = []

        try:
            import psutil
            self._psutil = psutil
            self._process = psutil.Process()
        except ImportError:
            self._psutil = None
            self._process = None
            self._logger.warning("psutil not available, memory monitoring limited")

    def get_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        if not self._process:
            return 0.0
        try:
            return self._process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def get_memory_percent(self) -> float:
        """Get memory usage as percentage of total system memory."""
        if not self._psutil:
            return 0.0
        try:
            return self._psutil.virtual_memory().percent
        except Exception:
            return 0.0

    def get_available_mb(self) -> float:
        """Get available system memory in MB."""
        if not self._psutil:
            return float('inf')
        try:
            return self._psutil.virtual_memory().available / (1024 * 1024)
        except Exception:
            return float('inf')

    def set_baseline(self):
        """Set baseline memory for delta tracking."""
        self._baseline_mb = self.get_memory_mb()
        self._logger.debug(f"Memory baseline set: {self._baseline_mb:.1f} MB")

    def get_delta_mb(self) -> float:
        """Get memory change from baseline."""
        return self.get_memory_mb() - self._baseline_mb

    def update_peak(self):
        """Update peak memory usage."""
        current = self.get_memory_mb()
        if current > self._peak_mb:
            self._peak_mb = current

    def record_sample(self):
        """Record memory sample for trend analysis."""
        current = self.get_memory_mb()
        self._samples.append(current)
        self.update_peak()

        # Keep last 100 samples
        if len(self._samples) > 100:
            self._samples = self._samples[-100:]

    def check_warning(self) -> bool:
        """Check if memory usage exceeds warning threshold."""
        current = self.get_memory_mb()
        if current > self._warning_threshold:
            self._logger.warning(
                f"Memory usage ({current:.1f} MB) exceeds threshold ({self._warning_threshold} MB)"
            )
            return True
        return False

    def get_trend(self) -> str:
        """Analyze memory usage trend."""
        if len(self._samples) < 10:
            return "stable"

        recent = self._samples[-10:]
        older = self._samples[:10] if len(self._samples) >= 20 else self._samples[:5]

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older)

        diff_percent = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0

        if diff_percent > 10:
            return "increasing"
        elif diff_percent < -10:
            return "decreasing"
        return "stable"

    def get_report(self) -> Dict:
        """Generate memory usage report."""
        return {
            'current_mb': round(self.get_memory_mb(), 2),
            'baseline_mb': round(self._baseline_mb, 2),
            'peak_mb': round(self._peak_mb, 2),
            'delta_mb': round(self.get_delta_mb(), 2),
            'available_mb': round(self.get_available_mb(), 2),
            'system_percent': round(self.get_memory_percent(), 1),
            'trend': self.get_trend(),
            'warning': self.check_warning()
        }

    def log_usage(self, context: str = ""):
        """Log current memory usage."""
        current = self.get_memory_mb()
        delta = self.get_delta_mb()
        msg = f"Memory: {current:.1f} MB"
        if self._baseline_mb > 0:
            msg += f" (delta: {delta:+.1f} MB)"
        if context:
            msg = f"[{context}] {msg}"
        self._logger.debug(msg)


class ChunkProcessor(Generic[T]):
    """Process data in memory-efficient chunks."""

    def __init__(
        self,
        chunk_size: int = 1000,
        memory_limit_mb: float = None,
        auto_adjust: bool = True
    ):
        self.chunk_size = chunk_size
        self.memory_limit_mb = memory_limit_mb or 200
        self.auto_adjust = auto_adjust
        self._logger = get_logger('chunk_processor')
        self._monitor = MemoryMonitor()

    def _adjust_chunk_size(self) -> int:
        """Dynamically adjust chunk size based on available memory."""
        if not self.auto_adjust:
            return self.chunk_size

        available = self._monitor.get_available_mb()
        current = self._monitor.get_memory_mb()

        # If using more than 80% of limit, reduce chunk size
        if current > self.memory_limit_mb * 0.8:
            new_size = max(100, self.chunk_size // 2)
            if new_size != self.chunk_size:
                self._logger.info(f"Reducing chunk size: {self.chunk_size} -> {new_size}")
                self.chunk_size = new_size

        # If plenty of memory, can increase
        elif current < self.memory_limit_mb * 0.3 and available > 500:
            new_size = min(5000, self.chunk_size * 2)
            if new_size != self.chunk_size:
                self._logger.info(f"Increasing chunk size: {self.chunk_size} -> {new_size}")
                self.chunk_size = new_size

        return self.chunk_size

    def process_list(
        self,
        items: List[T],
        processor: Callable[[List[T]], Any],
        progress_callback: Callable[[int, int], None] = None
    ) -> List[Any]:
        """Process list items in chunks."""
        results = []
        total = len(items)

        self._monitor.set_baseline()

        for i in range(0, total, self.chunk_size):
            chunk = items[i:i + self.chunk_size]

            result = processor(chunk)
            if result is not None:
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)

            # Progress callback
            processed = min(i + self.chunk_size, total)
            if progress_callback:
                progress_callback(processed, total)

            # Adjust and cleanup
            self._adjust_chunk_size()
            gc.collect()

        return results

    def process_dataframe(
        self,
        df: pd.DataFrame,
        processor: Callable[[pd.DataFrame], pd.DataFrame],
        progress_callback: Callable[[int, int], None] = None
    ) -> pd.DataFrame:
        """Process DataFrame in chunks."""
        if df.empty:
            return df

        results = []
        total = len(df)

        self._monitor.set_baseline()

        for start in range(0, total, self.chunk_size):
            end = min(start + self.chunk_size, total)
            chunk = df.iloc[start:end].copy()

            result = processor(chunk)
            if result is not None and not result.empty:
                results.append(result)

            if progress_callback:
                progress_callback(end, total)

            # Cleanup
            del chunk
            self._adjust_chunk_size()
            gc.collect()

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    def iterate_chunks(
        self,
        items: List[T]
    ) -> Iterator[List[T]]:
        """Generator for chunk iteration."""
        for i in range(0, len(items), self.chunk_size):
            yield items[i:i + self.chunk_size]
            gc.collect()


class DataOptimizer:
    """Optimizes DataFrame memory usage."""

    @staticmethod
    def optimize_dataframe(df: pd.DataFrame, copy: bool = True) -> pd.DataFrame:
        """Reduce DataFrame memory usage with type optimizations."""
        if df.empty:
            return df

        if copy:
            df = df.copy()

        start_mem = df.memory_usage(deep=True).sum() / (1024 * 1024)

        for col in df.columns:
            col_type = df[col].dtype

            # Optimize integers
            if col_type in ['int64', 'int32']:
                c_min = df[col].min()
                c_max = df[col].max()

                if c_min >= 0:
                    if c_max < 255:
                        df[col] = df[col].astype(np.uint8)
                    elif c_max < 65535:
                        df[col] = df[col].astype(np.uint16)
                    elif c_max < 4294967295:
                        df[col] = df[col].astype(np.uint32)
                else:
                    if c_min > -128 and c_max < 127:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > -32768 and c_max < 32767:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > -2147483648 and c_max < 2147483647:
                        df[col] = df[col].astype(np.int32)

            # Optimize floats
            elif col_type == 'float64':
                df[col] = df[col].astype(np.float32)

            # Convert object columns with few unique values to categorical
            elif col_type == 'object':
                num_unique = df[col].nunique()
                num_total = len(df[col])

                if num_unique / num_total < 0.5:  # Less than 50% unique
                    df[col] = df[col].astype('category')

        end_mem = df.memory_usage(deep=True).sum() / (1024 * 1024)

        logger = get_logger('optimizer')
        reduction = (1 - end_mem / start_mem) * 100 if start_mem > 0 else 0
        logger.debug(f"DataFrame optimized: {start_mem:.2f} MB -> {end_mem:.2f} MB ({reduction:.1f}% reduction)")

        return df

    @staticmethod
    def to_sparse(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
        """Convert columns to sparse representation."""
        if df.empty:
            return df

        df = df.copy()
        columns = columns or df.select_dtypes(include=[np.number]).columns.tolist()

        for col in columns:
            if col in df.columns:
                # Check if mostly zeros/NaN
                non_zero_ratio = (df[col] != 0).sum() / len(df[col])
                if non_zero_ratio < 0.3:  # Less than 30% non-zero
                    df[col] = pd.arrays.SparseArray(df[col].values, fill_value=0)

        return df


class DiskCache:
    """Disk-based cache for intermediate results."""

    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or tempfile.mkdtemp(prefix='scraper_cache_')
        self._logger = get_logger('disk_cache')
        self._keys: Dict[str, str] = {}

        os.makedirs(self.cache_dir, exist_ok=True)

    def save(self, key: str, data: pd.DataFrame):
        """Save DataFrame to disk cache."""
        path = os.path.join(self.cache_dir, f"{key}.parquet")
        data.to_parquet(path, compression='snappy')
        self._keys[key] = path
        self._logger.debug(f"Cached '{key}' to disk ({len(data)} rows)")

    def load(self, key: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from disk cache."""
        if key not in self._keys:
            return None

        path = self._keys[key]
        if not os.path.exists(path):
            return None

        self._logger.debug(f"Loading '{key}' from disk cache")
        return pd.read_parquet(path)

    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        return key in self._keys and os.path.exists(self._keys[key])

    def clear(self):
        """Clear all cached data."""
        import shutil
        try:
            shutil.rmtree(self.cache_dir)
            self._keys.clear()
            self._logger.debug("Disk cache cleared")
        except Exception as e:
            self._logger.warning(f"Failed to clear cache: {e}")

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self.clear()
        except Exception:
            pass


# Decorators

def memory_efficient(max_memory_mb: float = 200):
    """Decorator to ensure memory-efficient execution."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            monitor = MemoryMonitor(warning_threshold_mb=max_memory_mb)
            monitor.set_baseline()

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                monitor.log_usage(func.__name__)

                # Force cleanup if over limit
                if monitor.get_memory_mb() > max_memory_mb:
                    gc.collect()

        return wrapper
    return decorator


def cleanup_resources(func: Callable) -> Callable:
    """Decorator to ensure proper resource cleanup."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            # Force garbage collection
            gc.collect()

            # Clear matplotlib figures if used
            try:
                import matplotlib.pyplot as plt
                plt.close('all')
            except ImportError:
                pass

    return wrapper


@contextmanager
def memory_limit_context(max_mb: float = 200):
    """Context manager for memory-limited operations."""
    monitor = MemoryMonitor(warning_threshold_mb=max_mb)
    monitor.set_baseline()

    try:
        yield monitor
    finally:
        if monitor.check_warning():
            gc.collect()
        monitor.log_usage("context_exit")


# Weak Reference Cache

class WeakCache:
    """Cache using weak references (auto-cleanup when memory needed)."""

    def __init__(self):
        self._cache: Dict[str, weakref.ref] = {}

    def set(self, key: str, value: Any):
        """Store value with weak reference."""
        self._cache[key] = weakref.ref(value)

    def get(self, key: str) -> Optional[Any]:
        """Get value if still in memory."""
        if key not in self._cache:
            return None

        ref = self._cache[key]
        value = ref()

        if value is None:
            del self._cache[key]

        return value

    def clear(self):
        """Clear cache."""
        self._cache.clear()


# Global monitor instance
_memory_monitor: Optional[MemoryMonitor] = None

def get_memory_monitor() -> MemoryMonitor:
    """Get global memory monitor instance."""
    global _memory_monitor
    if _memory_monitor is None:
        _memory_monitor = MemoryMonitor()
    return _memory_monitor
