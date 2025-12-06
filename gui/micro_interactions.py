"""
GUI Micro-Interactions
Subtle animations and effects for enhanced user experience
"""

from PySide6.QtCore import QObject, QEvent, QPropertyAnimation, QEasingCurve, Qt
from PySide6.QtWidgets import QWidget, QGraphicsOpacityEffect, QGraphicsDropShadowEffect
from PySide6.QtGui import QColor, QCursor

from .constants import ANIMATION, COLORS


class HoverEffect(QObject):
    """
    Hover effect that scales widget on mouse enter/leave
    Install as event filter on any widget
    """

    def __init__(self, widget, scale_factor=1.05, duration=150, shadow=True):
        """
        Args:
            widget: Widget to apply hover effect
            scale_factor: Scale multiplier on hover
            duration: Animation duration in ms
            shadow: Whether to add shadow effect
        """
        super().__init__(widget)
        self.widget = widget
        self.scale_factor = scale_factor
        self.duration = duration
        self._original_size = None
        self._animation = None

        # Add shadow effect if requested
        if shadow:
            self.shadow = QGraphicsDropShadowEffect()
            self.shadow.setBlurRadius(10)
            self.shadow.setColor(QColor(0, 0, 0, 80))
            self.shadow.setOffset(0, 2)
            widget.setGraphicsEffect(self.shadow)
        else:
            self.shadow = None

        # Install event filter
        widget.installEventFilter(self)

    def eventFilter(self, obj, event):
        """Handle mouse enter/leave events"""
        if obj == self.widget:
            if event.type() == QEvent.Enter:
                self._on_hover_enter()
            elif event.type() == QEvent.Leave:
                self._on_hover_leave()

        return super().eventFilter(obj, event)

    def _on_hover_enter(self):
        """Handle mouse enter"""
        if self._original_size is None:
            self._original_size = self.widget.size()

        # Animate to scaled size
        target_size = self._original_size * self.scale_factor
        self._animate_size(target_size)

        # Enhance shadow
        if self.shadow:
            self.shadow.setBlurRadius(15)
            self.shadow.setOffset(0, 4)

        # Change cursor
        self.widget.setCursor(QCursor(Qt.PointingHandCursor))

    def _on_hover_leave(self):
        """Handle mouse leave"""
        if self._original_size:
            # Animate back to original size
            self._animate_size(self._original_size)

        # Reset shadow
        if self.shadow:
            self.shadow.setBlurRadius(10)
            self.shadow.setOffset(0, 2)

        # Reset cursor
        self.widget.setCursor(QCursor(Qt.ArrowCursor))

    def _animate_size(self, target_size):
        """Animate widget size"""
        if self._animation:
            self._animation.stop()

        self._animation = QPropertyAnimation(self.widget, b"size")
        self._animation.setDuration(self.duration)
        self._animation.setEasingCurve(QEasingCurve.OutCubic)
        self._animation.setStartValue(self.widget.size())
        self._animation.setEndValue(target_size)
        self._animation.start()


class PressEffect(QObject):
    """
    Press effect that scales down widget on click
    Install as event filter on any widget
    """

    def __init__(self, widget, scale_factor=0.95, duration=100):
        """
        Args:
            widget: Widget to apply press effect
            scale_factor: Scale multiplier on press
            duration: Animation duration in ms
        """
        super().__init__(widget)
        self.widget = widget
        self.scale_factor = scale_factor
        self.duration = duration
        self._original_size = None
        self._animation = None

        # Install event filter
        widget.installEventFilter(self)

    def eventFilter(self, obj, event):
        """Handle mouse press/release events"""
        if obj == self.widget:
            if event.type() == QEvent.MouseButtonPress:
                self._on_press()
            elif event.type() == QEvent.MouseButtonRelease:
                self._on_release()

        return super().eventFilter(obj, event)

    def _on_press(self):
        """Handle mouse press"""
        if self._original_size is None:
            self._original_size = self.widget.size()

        # Animate to scaled down size
        target_size = self._original_size * self.scale_factor
        self._animate_size(target_size)

    def _on_release(self):
        """Handle mouse release"""
        if self._original_size:
            # Animate back to original size
            self._animate_size(self._original_size)

    def _animate_size(self, target_size):
        """Animate widget size"""
        if self._animation:
            self._animation.stop()

        self._animation = QPropertyAnimation(self.widget, b"size")
        self._animation.setDuration(self.duration)
        self._animation.setEasingCurve(QEasingCurve.OutCubic)
        self._animation.setStartValue(self.widget.size())
        self._animation.setEndValue(target_size)
        self._animation.start()


class GlowEffect(QObject):
    """
    Glow effect that pulses shadow on focus
    Install as event filter on any widget
    """

    def __init__(self, widget, color=None, duration=1000):
        """
        Args:
            widget: Widget to apply glow effect
            color: Glow color (default: primary color)
            duration: Pulse duration in ms
        """
        super().__init__(widget)
        self.widget = widget
        self.duration = duration
        self._animation = None

        # Create shadow effect
        self.shadow = QGraphicsDropShadowEffect()
        self.shadow.setBlurRadius(0)
        self.shadow.setColor(QColor(color or COLORS['primary']))
        self.shadow.setOffset(0, 0)
        widget.setGraphicsEffect(self.shadow)

        # Install event filter
        widget.installEventFilter(self)

    def eventFilter(self, obj, event):
        """Handle focus in/out events"""
        if obj == self.widget:
            if event.type() == QEvent.FocusIn:
                self._start_glow()
            elif event.type() == QEvent.FocusOut:
                self._stop_glow()

        return super().eventFilter(obj, event)

    def _start_glow(self):
        """Start glow animation"""
        if self._animation:
            self._animation.stop()

        self._animation = QPropertyAnimation(self.shadow, b"blurRadius")
        self._animation.setDuration(self.duration)
        self._animation.setEasingCurve(QEasingCurve.InOutSine)
        self._animation.setStartValue(0)
        self._animation.setEndValue(20)
        self._animation.setLoopCount(-1)  # Infinite loop
        self._animation.start()

    def _stop_glow(self):
        """Stop glow animation"""
        if self._animation:
            self._animation.stop()
            self.shadow.setBlurRadius(0)


class RippleEffect:
    """
    Ripple effect on click (Material Design style)
    Note: This is a simplified version, full implementation would use QPainter
    """

    @staticmethod
    def create_ripple(widget, pos, color=None, duration=600):
        """
        Create ripple effect at position

        Args:
            widget: Widget to show ripple on
            pos: Click position (QPoint)
            color: Ripple color
            duration: Animation duration in ms
        """
        # This would require custom painting with QPainter
        # For now, we'll use a simple opacity fade as placeholder

        # Create overlay widget
        from PySide6.QtWidgets import QLabel
        overlay = QLabel(widget)
        overlay.setStyleSheet(f"""
            background-color: {color or COLORS['primary']};
            border-radius: 50px;
        """)
        overlay.resize(10, 10)
        overlay.move(pos.x() - 5, pos.y() - 5)
        overlay.show()

        # Create opacity effect
        effect = QGraphicsOpacityEffect(overlay)
        overlay.setGraphicsEffect(effect)

        # Animate opacity
        anim = QPropertyAnimation(effect, b"opacity")
        anim.setDuration(duration)
        anim.setEasingCurve(QEasingCurve.OutCubic)
        anim.setStartValue(0.5)
        anim.setEndValue(0.0)

        # Delete overlay when animation finishes
        anim.finished.connect(overlay.deleteLater)
        anim.start()


class LoadingMorph:
    """
    Morphing loading animation for progress indicators
    Smoothly transitions between states
    """

    @staticmethod
    def morph_progress(progress_widget, from_value, to_value, duration=300):
        """
        Morph progress value smoothly

        Args:
            progress_widget: Widget with setValue method
            from_value: Starting value
            to_value: Target value
            duration: Animation duration in ms
        """
        anim = QPropertyAnimation(progress_widget, b"value")
        anim.setDuration(duration)
        anim.setEasingCurve(QEasingCurve.InOutQuad)
        anim.setStartValue(from_value)
        anim.setEndValue(to_value)
        anim.start()

        return anim


def apply_hover_effect(widget, scale=1.05, shadow=True):
    """
    Convenience function to apply hover effect to widget

    Args:
        widget: Widget to apply effect
        scale: Scale factor on hover
        shadow: Whether to add shadow

    Returns:
        HoverEffect: The created effect instance
    """
    return HoverEffect(widget, scale_factor=scale, shadow=shadow)


def apply_press_effect(widget, scale=0.95):
    """
    Convenience function to apply press effect to widget

    Args:
        widget: Widget to apply effect
        scale: Scale factor on press

    Returns:
        PressEffect: The created effect instance
    """
    return PressEffect(widget, scale_factor=scale)


def apply_glow_effect(widget, color=None):
    """
    Convenience function to apply glow effect to widget

    Args:
        widget: Widget to apply effect
        color: Glow color

    Returns:
        GlowEffect: The created effect instance
    """
    return GlowEffect(widget, color=color)
