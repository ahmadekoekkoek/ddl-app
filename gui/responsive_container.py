"""
GUI Responsive Container
Container widget that adapts to screen size changes
"""

from PySide6.QtWidgets import QWidget, QVBoxLayout
from PySide6.QtCore import Qt, QTimer
from .responsive import ResponsiveScaler


class ResponsiveContainer(QWidget):
    """
    Container widget that automatically adapts to screen size changes
    Applies responsive scaling to all child widgets
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.scaler = ResponsiveScaler()
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)

        # Connect to breakpoint changes
        self.scaler.breakpoint_changed.connect(self._on_breakpoint_changed)
        self.scaler.scale_changed.connect(self._on_scale_changed)

        # Initial calculation
        QTimer.singleShot(100, self._update_responsive_state)

    def _update_responsive_state(self):
        """Update responsive state based on current screen size"""
        self.scaler.calculate_breakpoint()
        self.scaler.calculate_scale_factor()

    def _on_breakpoint_changed(self, breakpoint):
        """Handle breakpoint change"""
        print(f"[ResponsiveContainer] Breakpoint changed to: {breakpoint}")
        self._apply_responsive_styles()

    def _on_scale_changed(self, scale):
        """Handle scale factor change"""
        print(f"[ResponsiveContainer] Scale changed to: {scale:.2f}")
        self._apply_responsive_styles()

    def _apply_responsive_styles(self):
        """Apply responsive styles to container and children"""
        # Override in subclasses to apply custom responsive behavior
        pass

    def get_scaler(self):
        """Get the responsive scaler instance"""
        return self.scaler

    def is_mobile(self):
        """Check if in mobile breakpoint"""
        return self.scaler.is_mobile()

    def is_tablet(self):
        """Check if in tablet breakpoint"""
        return self.scaler.is_tablet()

    def is_desktop(self):
        """Check if in desktop or larger breakpoint"""
        return self.scaler.is_desktop()

    def resizeEvent(self, event):
        """Handle resize events to update responsive state"""
        super().resizeEvent(event)
        # Debounce resize events
        if not hasattr(self, '_resize_timer'):
            self._resize_timer = QTimer()
            self._resize_timer.setSingleShot(True)
            self._resize_timer.timeout.connect(self._update_responsive_state)
        self._resize_timer.start(150)  # 150ms debounce
