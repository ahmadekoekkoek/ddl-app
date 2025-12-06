"""
Progress Tracker
Manages progress callbacks, ETA calculations, and metrics.
"""

import time
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from core import get_logger


@dataclass
class ProgressMetrics:
    """Metrics for progress tracking."""
    stage: str = ""
    current: int = 0
    total: int = 0
    start_time: float = 0
    items_per_second: float = 0
    eta_seconds: float = 0
    percent_complete: float = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'stage': self.stage,
            'current': self.current,
            'total': self.total,
            'percent': self.percent_complete,
            'eta_seconds': self.eta_seconds,
            'items_per_second': self.items_per_second
        }


class ProgressTracker:
    """Tracks progress with ETA calculations and speed metrics."""

    def __init__(self, callback: Callable[[str, int, int], None] = None):
        self._logger = get_logger('progress_tracker')
        self.callback = callback

        # Timing
        self.start_time: float = 0
        self.stage_start_time: float = 0

        # Counters
        self.total_entities: int = 0
        self.completed_entities: int = 0
        self.current_stage: str = ""

        # Speed tracking
        self._speed_samples: list = []
        self._last_update_time: float = 0
        self._last_update_count: int = 0

    def start(self, total_entities: int):
        """Start tracking a new pipeline run."""
        self.start_time = time.time()
        self.total_entities = total_entities
        self.completed_entities = 0
        self._speed_samples = []
        self._logger.info(f"Started tracking {total_entities} entities")

    def start_stage(self, stage_name: str, total_items: int):
        """Start tracking a new stage."""
        self.current_stage = stage_name
        self.stage_start_time = time.time()
        self._last_update_time = self.stage_start_time
        self._last_update_count = 0
        self._logger.debug(f"Stage started: {stage_name} ({total_items} items)")

    def update(self, stage: str, current: int, total: int):
        """Update progress and call callback."""
        now = time.time()

        # Calculate speed
        if current > self._last_update_count and now > self._last_update_time:
            items_delta = current - self._last_update_count
            time_delta = now - self._last_update_time
            speed = items_delta / time_delta if time_delta > 0 else 0

            self._speed_samples.append(speed)
            if len(self._speed_samples) > 10:
                self._speed_samples.pop(0)

            self._last_update_time = now
            self._last_update_count = current

        # Call user callback
        if self.callback:
            try:
                self.callback(stage, current, total)
            except Exception as e:
                self._logger.warning(f"Progress callback error: {e}")

    def get_metrics(self) -> ProgressMetrics:
        """Get current progress metrics."""
        now = time.time()

        # Average speed from samples
        avg_speed = sum(self._speed_samples) / len(self._speed_samples) if self._speed_samples else 0

        # Calculate ETA
        remaining = self.total_entities - self.completed_entities
        eta = remaining / avg_speed if avg_speed > 0 else 0

        # Percent complete
        pct = (self.completed_entities / self.total_entities * 100) if self.total_entities > 0 else 0

        return ProgressMetrics(
            stage=self.current_stage,
            current=self.completed_entities,
            total=self.total_entities,
            start_time=self.start_time,
            items_per_second=avg_speed,
            eta_seconds=eta,
            percent_complete=pct
        )

    def format_eta(self) -> str:
        """Format ETA as human-readable string."""
        metrics = self.get_metrics()

        if metrics.eta_seconds <= 0:
            return "calculating..."

        eta = timedelta(seconds=int(metrics.eta_seconds))

        if eta.total_seconds() < 60:
            return f"{int(eta.total_seconds())}s"
        elif eta.total_seconds() < 3600:
            return f"{int(eta.total_seconds() // 60)}m {int(eta.total_seconds() % 60)}s"
        else:
            hours = int(eta.total_seconds() // 3600)
            mins = int((eta.total_seconds() % 3600) // 60)
            return f"{hours}h {mins}m"

    def format_speed(self) -> str:
        """Format speed as items/sec."""
        metrics = self.get_metrics()
        return f"{metrics.items_per_second:.1f} items/sec"

    def complete_stage(self, stage_name: str):
        """Mark a stage as complete."""
        elapsed = time.time() - self.stage_start_time
        self._logger.info(f"Stage complete: {stage_name} ({elapsed:.1f}s)")

    def complete(self) -> Dict[str, Any]:
        """Mark pipeline as complete and return summary."""
        elapsed = time.time() - self.start_time

        summary = {
            'total_entities': self.total_entities,
            'completed_entities': self.completed_entities,
            'elapsed_seconds': elapsed,
            'avg_items_per_second': self.completed_entities / elapsed if elapsed > 0 else 0,
            'success_rate': (self.completed_entities / self.total_entities * 100) if self.total_entities > 0 else 0
        }

        self._logger.info(
            f"Pipeline complete: {self.completed_entities}/{self.total_entities} entities "
            f"in {elapsed:.1f}s ({summary['avg_items_per_second']:.1f} items/sec)"
        )

        return summary

    def increment(self, count: int = 1):
        """Increment completed entity count."""
        self.completed_entities += count

    def get_elapsed(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self.start_time if self.start_time else 0

    def get_stage_elapsed(self) -> float:
        """Get elapsed time for current stage."""
        return time.time() - self.stage_start_time if self.stage_start_time else 0

    # File generation progress methods

    def file_generated(self, filename: str, file_type: str = "file"):
        """Track when a file is successfully generated."""
        self._logger.info(f"[FILE] Generated {file_type}: {filename}")
        if self.callback:
            try:
                self.callback(f"[FILE] {file_type}: {filename}", 1, 1)
            except Exception as e:
                self._logger.warning(f"File callback error: {e}")

    def csv_generated(self, filename: str):
        """Track CSV file generation."""
        self.file_generated(filename, "CSV")

    def xlsx_generated(self, filename: str):
        """Track XLSX file generation."""
        self.file_generated(filename, "XLSX")

    def pdf_generated(self, filename: str):
        """Track PDF file generation."""
        self.file_generated(filename, "PDF")
