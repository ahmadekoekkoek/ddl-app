"""
GUI Responsive Scaler
Dynamic scaling system for responsive UI across different screen sizes
"""

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QObject, Signal, QSize
from .constants import BREAKPOINTS


class ResponsiveScaler(QObject):
    """
    Dynamic scaling system that adapts UI to screen size
    Supports breakpoints: mobile, tablet, desktop, widescreen, ultra
    """

    # Signals emitted when breakpoint changes
    breakpoint_changed = Signal(str)  # Emits breakpoint name
    scale_changed = Signal(float)  # Emits scale factor

    def __init__(self):
        super().__init__()
        self._current_breakpoint = None
        self._scale_factor = 1.0
        self._base_width = BREAKPOINTS['desktop']  # 1920px baseline

    def get_screen_size(self):
        """Get current screen size"""
        screen = QApplication.primaryScreen()
        if screen:
            geometry = screen.availableGeometry()
            return QSize(geometry.width(), geometry.height())
        return QSize(1920, 1080)  # Fallback

    def calculate_breakpoint(self, width=None):
        """
        Calculate current breakpoint based on screen width

        Args:
            width: Screen width in pixels (auto-detected if None)

        Returns:
            str: Breakpoint name ('mobile', 'tablet', 'desktop', 'widescreen', 'ultra')
        """
        if width is None:
            width = self.get_screen_size().width()

        # Determine breakpoint
        if width < BREAKPOINTS['tablet']:
            breakpoint = 'mobile'
        elif width < BREAKPOINTS['desktop']:
            breakpoint = 'tablet'
        elif width < BREAKPOINTS['widescreen']:
            breakpoint = 'desktop'
        elif width < BREAKPOINTS['ultra']:
            breakpoint = 'widescreen'
        else:
            breakpoint = 'ultra'

        # Emit signal if breakpoint changed
        if breakpoint != self._current_breakpoint:
            self._current_breakpoint = breakpoint
            self.breakpoint_changed.emit(breakpoint)
            print(f"[ResponsiveScaler] Breakpoint changed to: {breakpoint} ({width}px)")

        return breakpoint

    def calculate_scale_factor(self, width=None):
        """
        Calculate scale factor based on screen width

        Args:
            width: Screen width in pixels (auto-detected if None)

        Returns:
            float: Scale factor (0.5 to 2.0)
        """
        if width is None:
            width = self.get_screen_size().width()

        # Calculate scale factor relative to base width (1920px)
        scale = width / self._base_width

        # Clamp scale factor to reasonable range
        scale = max(0.5, min(2.0, scale))

        # Emit signal if scale changed significantly (>5% change)
        if abs(scale - self._scale_factor) > 0.05:
            self._scale_factor = scale
            self.scale_changed.emit(scale)
            print(f"[ResponsiveScaler] Scale factor changed to: {scale:.2f}")

        return scale

    def get_scaled_value(self, base_value, width=None):
        """
        Get scaled value based on current screen size

        Args:
            base_value: Base value at 1920px width
            width: Screen width (auto-detected if None)

        Returns:
            int: Scaled value
        """
        scale = self.calculate_scale_factor(width)
        return int(base_value * scale)

    def get_font_size(self, base_size, width=None):
        """
        Get scaled font size with minimum threshold

        Args:
            base_size: Base font size at 1920px
            width: Screen width (auto-detected if None)

        Returns:
            int: Scaled font size (minimum 8px)
        """
        scaled = self.get_scaled_value(base_size, width)
        return max(8, scaled)  # Minimum 8px for readability

    def get_spacing(self, base_spacing, width=None):
        """
        Get scaled spacing value

        Args:
            base_spacing: Base spacing at 1920px
            width: Screen width (auto-detected if None)

        Returns:
            int: Scaled spacing
        """
        return self.get_scaled_value(base_spacing, width)

    def get_button_size(self, base_width, base_height, width=None):
        """
        Get scaled button size

        Args:
            base_width: Base button width at 1920px
            base_height: Base button height at 1920px
            width: Screen width (auto-detected if None)

        Returns:
            tuple: (scaled_width, scaled_height)
        """
        scale = self.calculate_scale_factor(width)
        return (
            int(base_width * scale),
            int(base_height * scale)
        )

    def is_mobile(self):
        """Check if current breakpoint is mobile"""
        self.calculate_breakpoint()
        return self._current_breakpoint == 'mobile'

    def is_tablet(self):
        """Check if current breakpoint is tablet"""
        self.calculate_breakpoint()
        return self._current_breakpoint == 'tablet'

    def is_desktop(self):
        """Check if current breakpoint is desktop or larger"""
        self.calculate_breakpoint()
        return self._current_breakpoint in ['desktop', 'widescreen', 'ultra']

    def is_widescreen(self):
        """Check if current breakpoint is widescreen or ultra"""
        self.calculate_breakpoint()
        return self._current_breakpoint in ['widescreen', 'ultra']

    def get_current_breakpoint(self):
        """Get current breakpoint name"""
        self.calculate_breakpoint()
        return self._current_breakpoint

    def get_current_scale(self):
        """Get current scale factor"""
        self.calculate_scale_factor()
        return self._scale_factor
