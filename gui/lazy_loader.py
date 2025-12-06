"""
GUI Lazy Widget Loader
Lazy loading system for heavy widgets with loading skeletons
"""

from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PySide6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QColor, QPainter


class LoadingSkeleton(QWidget):
    """Animated loading skeleton/shimmer effect"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(200, 100)
        self._shimmer_position = 0

        # Setup animation
        self.animation = QPropertyAnimation(self, b"shimmerPosition")
        self.animation.setDuration(1500)
        self.animation.setStartValue(0)
        self.animation.setEndValue(self.width())
        self.animation.setEasingCurve(QEasingCurve.InOutQuad)
        self.animation.setLoopCount(-1)  # Infinite loop
        self.animation.start()

    def get_shimmer_position(self):
        return self._shimmer_position

    def set_shimmer_position(self, pos):
        self._shimmer_position = pos
        self.update()

    shimmerPosition = property(get_shimmer_position, set_shimmer_position)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Background
        painter.fillRect(self.rect(), QColor(37, 37, 56))  # #252538

        # Shimmer effect
        gradient_width = 100
        shimmer_x = self._shimmer_position - gradient_width

        # Draw shimmer gradient
        from PySide6.QtGui import QLinearGradient
        gradient = QLinearGradient(shimmer_x, 0, shimmer_x + gradient_width, 0)
        gradient.setColorAt(0.0, QColor(37, 37, 56, 0))
        gradient.setColorAt(0.5, QColor(255, 255, 255, 30))
        gradient.setColorAt(1.0, QColor(37, 37, 56, 0))

        painter.fillRect(self.rect(), gradient)


class LazyWidgetLoader(QWidget):
    """Lazy load heavy widgets with loading skeleton"""

    def __init__(self, widget_factory, parent=None, delay_ms=100):
        """
        Args:
            widget_factory: Callable that returns the widget to load
            parent: Parent widget
            delay_ms: Delay before loading widget (allows UI to render first)
        """
        super().__init__(parent)
        self.widget_factory = widget_factory
        self._widget = None
        self._loading = True
        self.delay_ms = delay_ms

        self.setup_skeleton()

        # Schedule widget loading
        QTimer.singleShot(delay_ms, self.load_widget)

    def setup_skeleton(self):
        """Show loading shimmer while widget loads"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Loading skeleton
        self.skeleton = LoadingSkeleton()
        layout.addWidget(self.skeleton)

        # Loading text
        self.loading_label = QLabel("⏳ Memuat...")
        self.loading_label.setStyleSheet("font-size: 14px; color: #f39c12;")
        self.loading_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.loading_label)

    def load_widget(self):
        """Asynchronously load the actual widget"""
        if self._widget is None and self._loading:
            try:
                # Create the actual widget
                self._widget = self.widget_factory()

                # Remove skeleton
                if self.skeleton:
                    self.skeleton.animation.stop()
                    self.skeleton.setParent(None)
                    self.skeleton.deleteLater()
                    self.skeleton = None

                if self.loading_label:
                    self.loading_label.setParent(None)
                    self.loading_label.deleteLater()
                    self.loading_label = None

                # Add actual widget
                layout = self.layout()
                layout.addWidget(self._widget)

                self._loading = False
                print(f"[LazyWidgetLoader] Widget loaded successfully")

            except Exception as e:
                print(f"[LazyWidgetLoader] Error loading widget: {e}")
                if self.loading_label:
                    self.loading_label.setText(f"❌ Error: {str(e)[:50]}")
                    self.loading_label.setStyleSheet("font-size: 14px; color: #e74c3c;")

    def get_widget(self):
        """Get the loaded widget (or None if still loading)"""
        return self._widget

    def is_loaded(self):
        """Check if widget is loaded"""
        return not self._loading
