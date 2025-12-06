"""
Comprehensive GUI Testing Suite
Automated testing, visual regression, performance benchmarking, and responsive layout tests.
"""

import os
import sys
import time
import json
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
from datetime import datetime
from enum import Enum

from PySide6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QPushButton,
    QLineEdit, QTextEdit, QLabel
)
from PySide6.QtCore import QTimer, Qt, QRect, QSize
from PySide6.QtGui import QPixmap, QImage

from .constants import BREAKPOINTS, PERFORMANCE


class TestStatus(Enum):
    """Test result status."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestResult:
    """Result of a single test."""
    name: str
    status: str
    duration_ms: float = 0.0
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    screenshot_path: str = ""


@dataclass
class TestSuiteResult:
    """Result of a complete test suite run."""
    suite_name: str
    started_at: float = 0.0
    completed_at: float = 0.0
    tests: List[TestResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.tests)

    @property
    def passed(self) -> int:
        return sum(1 for t in self.tests if t.status == TestStatus.PASSED.value)

    @property
    def failed(self) -> int:
        return sum(1 for t in self.tests if t.status == TestStatus.FAILED.value)

    @property
    def duration_ms(self) -> float:
        return (self.completed_at - self.started_at) * 1000

    @property
    def success_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0.0


class ScreenshotCapture:
    """Handles screenshot capture for visual regression testing."""

    def __init__(self, output_dir: str = "test_screenshots"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def capture_widget(self, widget: QWidget, name: str) -> str:
        """Capture a screenshot of a widget."""
        pixmap = widget.grab()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        path = self.output_dir / filename
        pixmap.save(str(path))
        return str(path)

    def capture_window(self, window: QMainWindow, name: str) -> str:
        """Capture a screenshot of the entire window."""
        return self.capture_widget(window, name)

    def compare_images(self, path1: str, path2: str, threshold: float = 0.99) -> Tuple[bool, float]:
        """Compare two images and return similarity score."""
        img1 = QImage(path1)
        img2 = QImage(path2)

        if img1.size() != img2.size():
            return False, 0.0

        total_pixels = img1.width() * img1.height()
        matching = 0

        for x in range(img1.width()):
            for y in range(img1.height()):
                if img1.pixel(x, y) == img2.pixel(x, y):
                    matching += 1

        similarity = matching / total_pixels
        return similarity >= threshold, similarity


class GUITestCase:
    """Base class for GUI test cases."""

    def __init__(self, name: str):
        self.name = name
        self._window: Optional[QMainWindow] = None
        self._app: Optional[QApplication] = None

    def setup(self, window: QMainWindow):
        """Set up the test with the window reference."""
        self._window = window
        self._app = QApplication.instance()

    def teardown(self):
        """Clean up after the test."""
        pass

    def run(self) -> TestResult:
        """Run the test and return result."""
        raise NotImplementedError

    def find_widget(self, widget_type, name: str = None) -> Optional[QWidget]:
        """Find a widget by type and optionally by name."""
        if not self._window:
            return None

        widgets = self._window.findChildren(widget_type)
        if name:
            widgets = [w for w in widgets if w.objectName() == name]
        return widgets[0] if widgets else None

    def click(self, widget: QWidget):
        """Simulate a click on a widget."""
        if isinstance(widget, QPushButton):
            widget.click()
        QApplication.processEvents()

    def type_text(self, widget: QWidget, text: str):
        """Type text into an input widget."""
        if isinstance(widget, (QLineEdit, QTextEdit)):
            widget.clear()
            if isinstance(widget, QLineEdit):
                widget.setText(text)
            else:
                widget.setPlainText(text)
        QApplication.processEvents()

    def wait(self, ms: int = 100):
        """Wait for specified milliseconds."""
        end_time = time.time() + (ms / 1000)
        while time.time() < end_time:
            QApplication.processEvents()
            time.sleep(0.01)

    def assert_visible(self, widget: QWidget) -> bool:
        """Assert that a widget is visible."""
        return widget is not None and widget.isVisible()

    def assert_text(self, widget: QWidget, expected: str) -> bool:
        """Assert widget text matches expected."""
        if isinstance(widget, QLabel):
            return widget.text() == expected
        elif isinstance(widget, QLineEdit):
            return widget.text() == expected
        elif isinstance(widget, QTextEdit):
            return widget.toPlainText() == expected
        return False


class NavigationFlowTest(GUITestCase):
    """Test navigation through all stages."""

    def __init__(self):
        super().__init__("Navigation Flow Test")

    def run(self) -> TestResult:
        start = time.time()
        errors = []

        try:
            # Test stage navigation (if window has stack attribute)
            if hasattr(self._window, 'stack'):
                stack = self._window.stack
                total_stages = stack.count()

                for i in range(total_stages):
                    stack.setCurrentIndex(i)
                    self.wait(100)

                    current = stack.currentWidget()
                    if not self.assert_visible(current):
                        errors.append(f"Stage {i} not visible")

                # Return to first stage
                stack.setCurrentIndex(0)

            if errors:
                return TestResult(
                    name=self.name,
                    status=TestStatus.FAILED.value,
                    duration_ms=(time.time() - start) * 1000,
                    message="; ".join(errors)
                )

            return TestResult(
                name=self.name,
                status=TestStatus.PASSED.value,
                duration_ms=(time.time() - start) * 1000,
                message="All stages navigable"
            )

        except Exception as e:
            return TestResult(
                name=self.name,
                status=TestStatus.ERROR.value,
                duration_ms=(time.time() - start) * 1000,
                message=str(e)
            )


class PerformanceBenchmark:
    """Performance benchmarking for stages."""

    def __init__(self, window: QMainWindow):
        self.window = window
        self.results: Dict[str, Dict] = {}

    def measure_stage_load(self, stage_index: int, stage_name: str) -> Dict[str, float]:
        """Measure stage load time."""
        if not hasattr(self.window, 'stack'):
            return {}

        stack = self.window.stack

        # Force garbage collection
        import gc
        gc.collect()

        # Measure memory before
        try:
            import psutil
            process = psutil.Process()
            mem_before = process.memory_info().rss / (1024 * 1024)
        except:
            mem_before = 0

        # Measure load time
        start = time.perf_counter()
        stack.setCurrentIndex(stage_index)
        QApplication.processEvents()
        load_time = (time.perf_counter() - start) * 1000

        # Measure memory after
        try:
            mem_after = process.memory_info().rss / (1024 * 1024)
        except:
            mem_after = 0

        result = {
            "load_time_ms": load_time,
            "memory_delta_mb": mem_after - mem_before,
            "meets_target": load_time < PERFORMANCE['max_stage_load_ms']
        }

        self.results[stage_name] = result
        return result

    def measure_all_stages(self) -> Dict[str, Dict]:
        """Measure all stages."""
        if not hasattr(self.window, 'stack'):
            return {}

        stack = self.window.stack
        stage_names = ["Config", "Confirm", "Processing", "Package", "Terms", "Payment", "Success"]

        for i in range(min(stack.count(), len(stage_names))):
            self.measure_stage_load(i, stage_names[i])
            time.sleep(0.1)  # Brief pause between measurements

        return self.results

    def generate_report(self) -> Dict[str, Any]:
        """Generate a benchmark report."""
        total_load_time = sum(r.get("load_time_ms", 0) for r in self.results.values())
        slow_stages = [name for name, r in self.results.items() if not r.get("meets_target", True)]

        return {
            "stages": self.results,
            "summary": {
                "total_load_time_ms": total_load_time,
                "average_load_time_ms": total_load_time / len(self.results) if self.results else 0,
                "slow_stages": slow_stages,
                "all_meet_target": len(slow_stages) == 0
            }
        }


class ResponsiveLayoutTest:
    """Test responsive layouts at different screen sizes."""

    SCREEN_SIZES = [
        ("mobile", 375, 667),
        ("tablet", 768, 1024),
        ("desktop_hd", 1366, 768),
        ("desktop_fhd", 1920, 1080),
        ("4k", 3840, 2160),
    ]

    def __init__(self, window: QMainWindow, screenshot_dir: str = "responsive_tests"):
        self.window = window
        self.capture = ScreenshotCapture(screenshot_dir)
        self.results: List[Dict[str, Any]] = []

    def test_size(self, name: str, width: int, height: int) -> Dict[str, Any]:
        """Test layout at a specific size."""
        result = {
            "name": name,
            "size": f"{width}x{height}",
            "issues": []
        }

        # Resize window
        self.window.resize(width, height)
        QApplication.processEvents()
        time.sleep(0.2)  # Allow layout to settle

        # Check for overlapping widgets
        overlaps = self._check_overlaps()
        if overlaps:
            result["issues"].extend(overlaps)

        # Check text readability (widgets not too small)
        readability = self._check_readability()
        if readability:
            result["issues"].extend(readability)

        # Capture screenshot
        result["screenshot"] = self.capture.capture_window(self.window, f"responsive_{name}")

        result["passed"] = len(result["issues"]) == 0
        self.results.append(result)

        return result

    def _check_overlaps(self) -> List[str]:
        """Check for overlapping widgets."""
        issues = []

        widgets = self.window.findChildren(QWidget)
        visible_widgets = [w for w in widgets if w.isVisible() and w.width() > 0 and w.height() > 0]

        for i, w1 in enumerate(visible_widgets):
            for w2 in visible_widgets[i+1:]:
                # Skip parent-child relationships
                if w1.isAncestorOf(w2) or w2.isAncestorOf(w1):
                    continue

                r1 = w1.geometry()
                r2 = w2.geometry()

                # Map to global coordinates
                if w1.parent():
                    r1.translate(w1.parent().mapToGlobal(w1.pos()) - w1.mapToGlobal(QPoint(0, 0)))
                if w2.parent():
                    r2.translate(w2.parent().mapToGlobal(w2.pos()) - w2.mapToGlobal(QPoint(0, 0)))

                if r1.intersects(r2):
                    # Check if it's significant overlap (more than 10% of smaller widget)
                    intersection = r1.intersected(r2)
                    smaller_area = min(r1.width() * r1.height(), r2.width() * r2.height())
                    overlap_area = intersection.width() * intersection.height()

                    if smaller_area > 0 and overlap_area / smaller_area > 0.1:
                        issues.append(f"Overlap: {w1.objectName() or type(w1).__name__} and {w2.objectName() or type(w2).__name__}")

        return issues[:5]  # Limit to first 5 issues

    def _check_readability(self) -> List[str]:
        """Check text widget readability."""
        issues = []
        min_font_size = 10
        min_widget_height = 20

        text_widgets = self.window.findChildren((QLabel, QLineEdit, QPushButton))

        for widget in text_widgets:
            if not widget.isVisible():
                continue

            # Check minimum height
            if widget.height() < min_widget_height and widget.height() > 0:
                issues.append(f"Widget too small: {widget.objectName() or type(widget).__name__} ({widget.height()}px)")

            # Check font size
            font = widget.font()
            if font.pointSize() < min_font_size and font.pointSize() > 0:
                issues.append(f"Font too small: {widget.objectName() or type(widget).__name__} ({font.pointSize()}pt)")

        return issues[:5]

    def test_all_sizes(self) -> List[Dict[str, Any]]:
        """Test all defined screen sizes."""
        original_size = self.window.size()

        for name, width, height in self.SCREEN_SIZES:
            self.test_size(name, width, height)

        # Restore original size
        self.window.resize(original_size)

        return self.results

    def generate_report(self) -> Dict[str, Any]:
        """Generate responsive test report."""
        return {
            "tests": self.results,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r["passed"]),
                "failed": sum(1 for r in self.results if not r["passed"]),
                "all_passed": all(r["passed"] for r in self.results)
            }
        }


class GUITestSuite:
    """Main GUI testing orchestrator."""

    def __init__(self, window: QMainWindow, output_dir: str = "test_results"):
        self.window = window
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.test_cases: List[GUITestCase] = []
        self.screenshot_capture = ScreenshotCapture(str(self.output_dir / "screenshots"))
        self.results: Optional[TestSuiteResult] = None

    def add_test(self, test: GUITestCase):
        """Add a test case to the suite."""
        self.test_cases.append(test)

    def add_standard_tests(self):
        """Add standard test cases."""
        self.test_cases.extend([
            NavigationFlowTest(),
        ])

    def run(self) -> TestSuiteResult:
        """Run all tests in the suite."""
        self.results = TestSuiteResult(
            suite_name="GUI Test Suite",
            started_at=time.time()
        )

        for test in self.test_cases:
            test.setup(self.window)

            try:
                result = test.run()

                # Capture screenshot on failure
                if result.status == TestStatus.FAILED.value:
                    result.screenshot_path = self.screenshot_capture.capture_window(
                        self.window, f"failure_{test.name}"
                    )

                self.results.tests.append(result)

            except Exception as e:
                self.results.tests.append(TestResult(
                    name=test.name,
                    status=TestStatus.ERROR.value,
                    message=str(e)
                ))

            finally:
                test.teardown()

        self.results.completed_at = time.time()
        return self.results

    def run_performance_benchmark(self) -> Dict[str, Any]:
        """Run performance benchmarks."""
        benchmark = PerformanceBenchmark(self.window)
        benchmark.measure_all_stages()
        return benchmark.generate_report()

    def run_responsive_tests(self) -> Dict[str, Any]:
        """Run responsive layout tests."""
        responsive = ResponsiveLayoutTest(
            self.window,
            str(self.output_dir / "responsive")
        )
        responsive.test_all_sizes()
        return responsive.generate_report()

    def run_all(self) -> Dict[str, Any]:
        """Run all tests and return comprehensive report."""
        self.add_standard_tests()

        report = {
            "timestamp": datetime.now().isoformat(),
            "gui_tests": None,
            "performance": None,
            "responsive": None,
            "summary": {}
        }

        # Run GUI tests
        gui_results = self.run()
        report["gui_tests"] = {
            "total": gui_results.total,
            "passed": gui_results.passed,
            "failed": gui_results.failed,
            "duration_ms": gui_results.duration_ms,
            "tests": [asdict(t) for t in gui_results.tests]
        }

        # Run performance benchmarks
        report["performance"] = self.run_performance_benchmark()

        # Run responsive tests
        report["responsive"] = self.run_responsive_tests()

        # Summary
        all_passed = (
            gui_results.failed == 0 and
            report["performance"]["summary"]["all_meet_target"] and
            report["responsive"]["summary"]["all_passed"]
        )

        report["summary"] = {
            "all_passed": all_passed,
            "gui_pass_rate": gui_results.success_rate,
            "performance_pass": report["performance"]["summary"]["all_meet_target"],
            "responsive_pass": report["responsive"]["summary"]["all_passed"]
        }

        # Save report
        self._save_report(report)

        return report

    def _save_report(self, report: Dict[str, Any]):
        """Save test report to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.output_dir / f"test_report_{timestamp}.json"

        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)

    def print_summary(self):
        """Print test summary to console."""
        if not self.results:
            print("No test results available")
            return

        print("\n" + "=" * 50)
        print("GUI TEST SUITE RESULTS")
        print("=" * 50)
        print(f"Total Tests: {self.results.total}")
        print(f"Passed: {self.results.passed}")
        print(f"Failed: {self.results.failed}")
        print(f"Success Rate: {self.results.success_rate:.1f}%")
        print(f"Duration: {self.results.duration_ms:.0f}ms")
        print("=" * 50)

        if self.results.failed > 0:
            print("\nFailed Tests:")
            for test in self.results.tests:
                if test.status == TestStatus.FAILED.value:
                    print(f"  - {test.name}: {test.message}")


# Point import helper
from PySide6.QtCore import QPoint
