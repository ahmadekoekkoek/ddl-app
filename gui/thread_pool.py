"""
GUI Thread Pool Manager
Centralized thread pool for background operations
"""

from PySide6.QtCore import QThreadPool


class ThreadPoolManager:
    """Centralized thread pool for all background tasks"""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance of thread pool"""
        if cls._instance is None:
            cls._instance = QThreadPool.globalInstance()
            # Set optimal thread count for I/O-bound operations
            # 4 threads is optimal for network-heavy scraping tasks
            cls._instance.setMaxThreadCount(4)
            print(f"[ThreadPoolManager] Initialized with {cls._instance.maxThreadCount()} threads")
        return cls._instance

    @classmethod
    def set_max_threads(cls, count):
        """Set maximum thread count"""
        pool = cls.get_instance()
        pool.setMaxThreadCount(count)
        print(f"[ThreadPoolManager] Max threads set to {count}")

    @classmethod
    def get_active_thread_count(cls):
        """Get number of currently active threads"""
        pool = cls.get_instance()
        return pool.activeThreadCount()

    @classmethod
    def wait_for_done(cls, timeout_ms=-1):
        """Wait for all threads to finish"""
        pool = cls.get_instance()
        return pool.waitForDone(timeout_ms)
