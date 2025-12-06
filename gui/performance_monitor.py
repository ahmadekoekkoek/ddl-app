"""
Performance Monitoring System
Tracks frame rate, memory usage, response times, and provides optimization recommendations.
"""

import time
import threading
import psutil
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

from PySide6.QtCore import QObject, Signal, QTimer, QElapsedTimer
from PySide6.QtWidgets import QApplication

from .constants import PERFORMANCE


class PerformanceGrade(Enum):
    """Performance grading levels."""
    EXCELLENT = "A"
    GOOD = "B"
    ACCEPTABLE = "C"
    POOR = "D"
    CRITICAL = "F"


@dataclass
class PerformanceMetrics:
    """Container for performance measurements."""
    timestamp: float = 0.0
    fps: float = 0.0
    frame_time_ms: float = 0.0
    memory_mb: float = 0.0
    memory_percent: float = 0.0
    cpu_percent: float = 0.0
    response_time_ms: float = 0.0
    animation_smoothness: float = 100.0


@dataclass
class StageMetrics:
    """Performance metrics for a specific stage."""
    stage_name: str
    load_time_ms: float = 0.0
    render_time_ms: float = 0.0
    memory_delta_mb: float = 0.0
    is_slow: bool = False


@dataclass
class OptimizationRecommendation:
    """A performance optimization suggestion."""
    category: str
    severity: str  # 'warning', 'critical'
    message: str
    action: str


class FrameRateMonitor:
    """Monitors application frame rate and animation smoothness."""

    def __init__(self, sample_size: int = 60):
        self.sample_size = sample_size
        self.frame_times: deque = deque(maxlen=sample_size)
        self.last_frame_time: float = 0.0
        self._timer = QElapsedTimer()
        self._timer.start()

    def record_frame(self):
        """Record a frame render."""
        current_time = self._timer.elapsed()
        if self.last_frame_time > 0:
            delta = current_time - self.last_frame_time
            self.frame_times.append(delta)
        self.last_frame_time = current_time

    def get_fps(self) -> float:
        """Calculate current FPS from recent frames."""
        if len(self.frame_times) < 2:
            return 60.0
        avg_frame_time = sum(self.frame_times) / len(self.frame_times)
        if avg_frame_time <= 0:
            return 60.0
        return min(1000.0 / avg_frame_time, 120.0)

    def get_frame_time_ms(self) -> float:
        """Get average frame time in milliseconds."""
        if not self.frame_times:
            return 16.67
        return sum(self.frame_times) / len(self.frame_times)

    def get_animation_smoothness(self) -> float:
        """Calculate animation smoothness score (0-100)."""
        if len(self.frame_times) < 10:
            return 100.0

        target_frame_time = 1000.0 / PERFORMANCE['target_fps']

        # Calculate variance from target
        deviations = [abs(ft - target_frame_time) for ft in self.frame_times]
        avg_deviation = sum(deviations) / len(deviations)

        # Score based on deviation (lower is better)
        # 0ms deviation = 100%, 20ms+ deviation = 0%
        score = max(0, 100 - (avg_deviation * 5))
        return round(score, 1)


class MemoryMonitor:
    """Monitors application memory usage."""

    def __init__(self, warning_threshold_mb: float = None):
        self.warning_threshold = warning_threshold_mb or PERFORMANCE['max_memory_mb']
        self.process = psutil.Process()
        self.baseline_mb: float = 0.0
        self._history: deque = deque(maxlen=100)

    def set_baseline(self):
        """Record baseline memory usage."""
        self.baseline_mb = self.get_memory_mb()

    def get_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        try:
            return self.process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def get_memory_percent(self) -> float:
        """Get memory usage as percentage of system memory."""
        try:
            return self.process.memory_percent()
        except Exception:
            return 0.0

    def get_memory_delta(self) -> float:
        """Get memory change from baseline."""
        return self.get_memory_mb() - self.baseline_mb

    def record_sample(self):
        """Record a memory sample."""
        self._history.append(self.get_memory_mb())

    def is_warning(self) -> bool:
        """Check if memory usage exceeds warning threshold."""
        return self.get_memory_mb() > self.warning_threshold

    def get_trend(self) -> str:
        """Analyze memory trend."""
        if len(self._history) < 10:
            return "stable"

        recent = list(self._history)[-10:]
        older = list(self._history)[:10] if len(self._history) >= 20 else recent

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older)

        diff_percent = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0

        if diff_percent > 10:
            return "increasing"
        elif diff_percent < -10:
            return "decreasing"
        return "stable"


class ResponseTimeTracker:
    """Tracks response times for various operations."""

    def __init__(self):
        self._timers: Dict[str, float] = {}
        self._results: Dict[str, deque] = {}

    def start(self, operation: str):
        """Start timing an operation."""
        self._timers[operation] = time.perf_counter()

    def stop(self, operation: str) -> float:
        """Stop timing and return elapsed time in ms."""
        if operation not in self._timers:
            return 0.0

        elapsed = (time.perf_counter() - self._timers[operation]) * 1000

        if operation not in self._results:
            self._results[operation] = deque(maxlen=50)
        self._results[operation].append(elapsed)

        del self._timers[operation]
        return elapsed

    def get_average(self, operation: str) -> float:
        """Get average response time for an operation."""
        if operation not in self._results or not self._results[operation]:
            return 0.0
        return sum(self._results[operation]) / len(self._results[operation])

    def get_all_averages(self) -> Dict[str, float]:
        """Get average response times for all tracked operations."""
        return {op: self.get_average(op) for op in self._results}


class PerformanceMonitor(QObject):
    """Main performance monitoring system."""

    metrics_updated = Signal(PerformanceMetrics)
    warning_triggered = Signal(str, str)  # category, message
    recommendation_generated = Signal(OptimizationRecommendation)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.frame_monitor = FrameRateMonitor()
        self.memory_monitor = MemoryMonitor()
        self.response_tracker = ResponseTimeTracker()

        self._stage_metrics: Dict[str, StageMetrics] = {}
        self._current_stage: Optional[str] = None
        self._stage_start_time: float = 0.0
        self._stage_start_memory: float = 0.0

        self._monitoring = False
        self._update_interval = 1000  # ms

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._collect_metrics)

        self._recommendations: List[OptimizationRecommendation] = []

    def start(self):
        """Start performance monitoring."""
        self._monitoring = True
        self.memory_monitor.set_baseline()
        self._timer.start(self._update_interval)

    def stop(self):
        """Stop performance monitoring."""
        self._monitoring = False
        self._timer.stop()

    def record_frame(self):
        """Record a frame render (call from paint events)."""
        if self._monitoring:
            self.frame_monitor.record_frame()

    def start_stage(self, stage_name: str):
        """Start tracking a stage load."""
        self._current_stage = stage_name
        self._stage_start_time = time.perf_counter()
        self._stage_start_memory = self.memory_monitor.get_memory_mb()
        self.response_tracker.start(f"stage_{stage_name}")

    def end_stage(self, stage_name: str) -> StageMetrics:
        """End stage tracking and return metrics."""
        load_time = self.response_tracker.stop(f"stage_{stage_name}")
        memory_delta = self.memory_monitor.get_memory_mb() - self._stage_start_memory

        is_slow = load_time > PERFORMANCE['max_stage_load_ms']

        metrics = StageMetrics(
            stage_name=stage_name,
            load_time_ms=load_time,
            memory_delta_mb=memory_delta,
            is_slow=is_slow
        )

        self._stage_metrics[stage_name] = metrics

        if is_slow:
            self._generate_slow_stage_recommendation(stage_name, load_time)

        self._current_stage = None
        return metrics

    def time_operation(self, operation: str):
        """Context manager for timing operations."""
        return OperationTimer(self.response_tracker, operation)

    def _collect_metrics(self):
        """Collect and emit current metrics."""
        self.memory_monitor.record_sample()

        metrics = PerformanceMetrics(
            timestamp=time.time(),
            fps=self.frame_monitor.get_fps(),
            frame_time_ms=self.frame_monitor.get_frame_time_ms(),
            memory_mb=self.memory_monitor.get_memory_mb(),
            memory_percent=self.memory_monitor.get_memory_percent(),
            cpu_percent=self._get_cpu_percent(),
            animation_smoothness=self.frame_monitor.get_animation_smoothness()
        )

        self.metrics_updated.emit(metrics)
        self._check_thresholds(metrics)

    def _get_cpu_percent(self) -> float:
        """Get CPU usage percentage."""
        try:
            return psutil.Process().cpu_percent()
        except Exception:
            return 0.0

    def _check_thresholds(self, metrics: PerformanceMetrics):
        """Check metrics against thresholds and generate warnings."""
        # Memory warning
        if metrics.memory_mb > PERFORMANCE['max_memory_mb']:
            self.warning_triggered.emit(
                "memory",
                f"Memory usage ({metrics.memory_mb:.1f}MB) exceeds threshold ({PERFORMANCE['max_memory_mb']}MB)"
            )
            self._generate_memory_recommendation()

        # FPS warning
        if metrics.fps < PERFORMANCE['target_fps'] * 0.8:
            self.warning_triggered.emit(
                "fps",
                f"Frame rate ({metrics.fps:.1f} FPS) below target ({PERFORMANCE['target_fps']} FPS)"
            )

        # Animation smoothness
        if metrics.animation_smoothness < 70:
            self.warning_triggered.emit(
                "animation",
                f"Animation smoothness ({metrics.animation_smoothness}%) is degraded"
            )

    def _generate_slow_stage_recommendation(self, stage: str, load_time: float):
        """Generate recommendation for slow stage."""
        rec = OptimizationRecommendation(
            category="stage_load",
            severity="warning",
            message=f"Stage '{stage}' loaded slowly ({load_time:.0f}ms > {PERFORMANCE['max_stage_load_ms']}ms)",
            action="Consider lazy loading heavy components or deferring non-critical initialization"
        )
        self._recommendations.append(rec)
        self.recommendation_generated.emit(rec)

    def _generate_memory_recommendation(self):
        """Generate memory optimization recommendation."""
        trend = self.memory_monitor.get_trend()

        if trend == "increasing":
            rec = OptimizationRecommendation(
                category="memory",
                severity="critical",
                message="Memory usage is continuously increasing (potential memory leak)",
                action="Review object lifecycle, ensure proper cleanup of event handlers and large data structures"
            )
        else:
            rec = OptimizationRecommendation(
                category="memory",
                severity="warning",
                message=f"High memory usage ({self.memory_monitor.get_memory_mb():.1f}MB)",
                action="Consider reducing image sizes, implementing object pooling, or clearing unused caches"
            )

        self._recommendations.append(rec)
        self.recommendation_generated.emit(rec)

    def get_grade(self) -> PerformanceGrade:
        """Calculate overall performance grade."""
        score = 100

        # FPS impact (40 points)
        fps = self.frame_monitor.get_fps()
        fps_ratio = fps / PERFORMANCE['target_fps']
        score -= max(0, (1 - fps_ratio) * 40)

        # Memory impact (30 points)
        mem_mb = self.memory_monitor.get_memory_mb()
        mem_ratio = mem_mb / PERFORMANCE['max_memory_mb']
        if mem_ratio > 1:
            score -= 30
        else:
            score -= mem_ratio * 15

        # Animation smoothness (20 points)
        smoothness = self.frame_monitor.get_animation_smoothness()
        score -= (100 - smoothness) * 0.2

        # Stage load times (10 points)
        slow_stages = sum(1 for m in self._stage_metrics.values() if m.is_slow)
        score -= slow_stages * 5

        if score >= 90:
            return PerformanceGrade.EXCELLENT
        elif score >= 75:
            return PerformanceGrade.GOOD
        elif score >= 60:
            return PerformanceGrade.ACCEPTABLE
        elif score >= 40:
            return PerformanceGrade.POOR
        return PerformanceGrade.CRITICAL

    def get_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        return {
            "grade": self.get_grade().value,
            "fps": {
                "current": self.frame_monitor.get_fps(),
                "target": PERFORMANCE['target_fps'],
                "frame_time_ms": self.frame_monitor.get_frame_time_ms()
            },
            "memory": {
                "current_mb": self.memory_monitor.get_memory_mb(),
                "threshold_mb": PERFORMANCE['max_memory_mb'],
                "trend": self.memory_monitor.get_trend(),
                "percent": self.memory_monitor.get_memory_percent()
            },
            "animation_smoothness": self.frame_monitor.get_animation_smoothness(),
            "stage_metrics": {
                name: {
                    "load_time_ms": m.load_time_ms,
                    "memory_delta_mb": m.memory_delta_mb,
                    "is_slow": m.is_slow
                }
                for name, m in self._stage_metrics.items()
            },
            "response_times": self.response_tracker.get_all_averages(),
            "recommendations": [
                {
                    "category": r.category,
                    "severity": r.severity,
                    "message": r.message,
                    "action": r.action
                }
                for r in self._recommendations
            ]
        }

    def get_network_grade(self, latency_ms: float, bandwidth_mbps: float) -> str:
        """Grade network performance."""
        if latency_ms < 50 and bandwidth_mbps > 10:
            return "Excellent"
        elif latency_ms < 100 and bandwidth_mbps > 5:
            return "Good"
        elif latency_ms < 200 and bandwidth_mbps > 1:
            return "Acceptable"
        elif latency_ms < 500:
            return "Poor"
        return "Critical"


class OperationTimer:
    """Context manager for timing operations."""

    def __init__(self, tracker: ResponseTimeTracker, operation: str):
        self.tracker = tracker
        self.operation = operation
        self.elapsed: float = 0.0

    def __enter__(self):
        self.tracker.start(self.operation)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = self.tracker.stop(self.operation)
        return False


# Singleton instance
_performance_monitor: Optional[PerformanceMonitor] = None

def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    return _performance_monitor
