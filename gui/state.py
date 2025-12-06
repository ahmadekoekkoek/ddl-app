"""
GUI State Management Module
Centralized state management pattern for application state
"""

from PySide6.QtCore import QObject, Signal


class AppState(QObject):
    """Single source of truth for application state"""

    # Configuration state
    config_loaded = Signal(bool)
    config_data = Signal(dict)
    validation_complete = Signal(dict)

    # Payment state
    payment_status = Signal(str)  # 'pending', 'completed', 'skipped', 'failed'
    payment_data = Signal(dict)

    # Scraping state
    scraping_progress = Signal(int, int)  # current, total
    scraping_status = Signal(str)
    scraping_metrics = Signal(dict)  # speed, eta, etc.

    # Navigation state
    current_stage = Signal(int)
    stage_history = Signal(list)

    # Error state
    error_occurred = Signal(str, dict)  # error_type, details

    def __init__(self):
        super().__init__()
        self._config = {}
        self._payment_info = {}
        self._scraping_data = {}
        self._current_stage_index = 0
        self._history = []

    # Configuration methods
    def set_config(self, config_dict):
        """Set configuration data"""
        self._config = config_dict.copy()
        self.config_data.emit(self._config)
        self.config_loaded.emit(True)

    def get_config(self):
        """Get current configuration"""
        return self._config.copy()

    # Payment methods
    def set_payment_status(self, status):
        """Set payment status"""
        self.payment_status.emit(status)

    def set_payment_data(self, data):
        """Set payment data"""
        self._payment_info = data.copy()
        self.payment_data.emit(self._payment_info)

    # Scraping methods
    def update_scraping_progress(self, current, total):
        """Update scraping progress"""
        self.scraping_progress.emit(current, total)

    def update_scraping_status(self, status):
        """Update scraping status"""
        self.scraping_status.emit(status)

    def update_scraping_metrics(self, metrics):
        """Update scraping metrics (speed, eta, etc.)"""
        self._scraping_data = metrics.copy()
        self.scraping_metrics.emit(self._scraping_data)

    # Navigation methods
    def set_current_stage(self, index):
        """Set current stage index"""
        self._current_stage_index = index
        self._history.append(index)
        self.current_stage.emit(index)
        self.stage_history.emit(self._history.copy())

    def get_current_stage(self):
        """Get current stage index"""
        return self._current_stage_index

    # Error methods
    def emit_error(self, error_type, details):
        """Emit error with type and details"""
        self.error_occurred.emit(error_type, details)

    def reset(self):
        """Reset all state"""
        self._config = {}
        self._payment_info = {}
        self._scraping_data = {}
        self._current_stage_index = 0
        self._history = []
