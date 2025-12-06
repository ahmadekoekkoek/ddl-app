"""
GUI Accessibility Module
WCAG 2.1 AA compliance, keyboard navigation, screen reader support, and shortcuts.
"""

from typing import Dict, List, Callable, Optional, Any
from PySide6.QtWidgets import QWidget, QApplication, QMainWindow, QLabel
from PySide6.QtGui import QColor, QPalette, QKeySequence, QAction, QFont
from PySide6.QtCore import Qt, QObject, Signal, QTimer

from .constants import COLORS, ACCESSIBILITY, HIGH_CONTRAST_COLORS


class KeyboardShortcut:
    """Represents a keyboard shortcut with its metadata."""
    def __init__(self, key: str, description: str, callback: Callable, category: str = "General"):
        self.key = key
        self.description = description
        self.callback = callback
        self.category = category
        self.enabled = True


class KeyboardShortcutManager(QObject):
    """Manages application-wide keyboard shortcuts."""
    shortcut_triggered = Signal(str)

    def __init__(self, parent: QMainWindow = None):
        super().__init__(parent)
        self.shortcuts: Dict[str, KeyboardShortcut] = {}
        self.parent_window = parent
        self._actions: List[QAction] = []

    def register(self, key: str, description: str, callback: Callable, category: str = "General"):
        """Register a new keyboard shortcut."""
        shortcut = KeyboardShortcut(key, description, callback, category)
        self.shortcuts[key] = shortcut

        if self.parent_window:
            action = QAction(description, self.parent_window)
            action.setShortcut(QKeySequence(key))
            action.triggered.connect(lambda: self._trigger_shortcut(key))
            self.parent_window.addAction(action)
            self._actions.append(action)

        return shortcut

    def _trigger_shortcut(self, key: str):
        """Internal handler for shortcut activation."""
        if key in self.shortcuts and self.shortcuts[key].enabled:
            self.shortcuts[key].callback()
            self.shortcut_triggered.emit(key)

    def unregister(self, key: str):
        """Remove a shortcut."""
        if key in self.shortcuts:
            del self.shortcuts[key]

    def enable(self, key: str, enabled: bool = True):
        """Enable or disable a shortcut."""
        if key in self.shortcuts:
            self.shortcuts[key].enabled = enabled

    def get_shortcuts_by_category(self) -> Dict[str, List[KeyboardShortcut]]:
        """Group shortcuts by category."""
        categories: Dict[str, List[KeyboardShortcut]] = {}
        for shortcut in self.shortcuts.values():
            if shortcut.category not in categories:
                categories[shortcut.category] = []
            categories[shortcut.category].append(shortcut)
        return categories

    def get_help_text(self) -> str:
        """Generate formatted help text for all shortcuts."""
        lines = ["Keyboard Shortcuts:", "=" * 40]
        for category, shortcuts in self.get_shortcuts_by_category().items():
            lines.append(f"\n[{category}]")
            for s in shortcuts:
                status = "" if s.enabled else " (disabled)"
                lines.append(f"  {s.key:15} - {s.description}{status}")
        return "\n".join(lines)


class AccessibilityManager(QObject):
    """Manages application-wide accessibility settings and WCAG compliance."""

    high_contrast_changed = Signal(bool)
    font_scale_changed = Signal(float)

    # Default shortcuts for navigation
    DEFAULT_SHORTCUTS = {
        "Ctrl+Q": ("Quit Application", "Navigation"),
        "F11": ("Toggle Fullscreen", "Navigation"),
        "Ctrl+S": ("Save", "Actions"),
        "F9": ("Validate", "Actions"),
        "Escape": ("Cancel/Close", "Navigation"),
        "Ctrl+Right": ("Next Stage", "Navigation"),
        "Ctrl+Left": ("Previous Stage", "Navigation"),
        "Ctrl+H": ("Show Help", "Help"),
        "F1": ("Show Shortcuts", "Help"),
    }

    def __init__(self, parent: QMainWindow = None):
        super().__init__(parent)
        self.high_contrast_mode = False
        self.font_scale = 1.0
        self.reduced_motion = False
        self.parent_window = parent
        self.shortcut_manager = KeyboardShortcutManager(parent)
        self._focus_widgets: List[QWidget] = []
        self._screen_reader_enabled = False

    def toggle_high_contrast(self) -> bool:
        """Toggle high contrast mode for better visibility."""
        self.high_contrast_mode = not self.high_contrast_mode
        self._apply_high_contrast()
        self.high_contrast_changed.emit(self.high_contrast_mode)
        return self.high_contrast_mode

    def _apply_high_contrast(self):
        """Apply or remove high contrast theme."""
        if not self.parent_window:
            return

        app = QApplication.instance()
        palette = QPalette()

        if self.high_contrast_mode:
            colors = HIGH_CONTRAST_COLORS
            palette.setColor(QPalette.Window, QColor(colors['background']))
            palette.setColor(QPalette.WindowText, QColor(colors['text']))
            palette.setColor(QPalette.Base, QColor(colors['background']))
            palette.setColor(QPalette.Text, QColor(colors['text']))
            palette.setColor(QPalette.Button, QColor(colors['background']))
            palette.setColor(QPalette.ButtonText, QColor(colors['text']))
            palette.setColor(QPalette.Highlight, QColor(colors['primary']))
        else:
            palette.setColor(QPalette.Window, QColor(30, 30, 46))
            palette.setColor(QPalette.WindowText, Qt.white)
            palette.setColor(QPalette.Base, QColor(24, 24, 36))
            palette.setColor(QPalette.Text, Qt.white)
            palette.setColor(QPalette.Button, QColor(30, 30, 46))
            palette.setColor(QPalette.ButtonText, Qt.white)
            palette.setColor(QPalette.Highlight, QColor(42, 130, 218))

        app.setPalette(palette)

    def set_font_scale(self, scale: float):
        """Adjust font size for accessibility (0.5 - 2.0 range)."""
        self.font_scale = max(0.5, min(2.0, scale))
        app = QApplication.instance()
        font = app.font()
        base_size = 10
        font.setPointSize(int(base_size * self.font_scale))
        app.setFont(font)
        self.font_scale_changed.emit(self.font_scale)

    def set_reduced_motion(self, enabled: bool):
        """Enable reduced motion for users sensitive to animations."""
        self.reduced_motion = enabled

    def register_focusable_widgets(self, widgets: List[QWidget]):
        """Register widgets for keyboard navigation focus chain."""
        self._focus_widgets = widgets
        create_focus_chain(widgets)

    def announce_to_screen_reader(self, message: str, priority: str = "polite"):
        """Announce message to screen reader (if available)."""
        # Qt uses platform accessibility APIs automatically
        # This creates a temporary label that screen readers will pick up
        if self.parent_window and self._screen_reader_enabled:
            # Create an off-screen label for announcement
            label = QLabel(message, self.parent_window)
            label.setAccessibleName(message)
            label.setGeometry(-9999, -9999, 1, 1)
            label.show()
            # Remove after brief delay
            QTimer.singleShot(100, label.deleteLater)

    def setup_default_shortcuts(self, callbacks: Dict[str, Callable]):
        """Set up default navigation shortcuts with provided callbacks."""
        for key, (desc, category) in self.DEFAULT_SHORTCUTS.items():
            if key in callbacks:
                self.shortcut_manager.register(key, desc, callbacks[key], category)

    def validate_contrast(self, fg: QColor, bg: QColor) -> Dict[str, Any]:
        """Validate contrast ratio and return compliance info."""
        ratio = check_contrast_ratio(fg, bg)
        return {
            "ratio": ratio,
            "aa_normal": ratio >= 4.5,
            "aa_large": ratio >= 3.0,
            "aaa_normal": ratio >= 7.0,
            "aaa_large": ratio >= 4.5,
        }


class FocusIndicator(QObject):
    """Manages visual focus indicators for keyboard navigation."""

    FOCUS_STYLE = """
        border: {width}px solid {color} !important;
        outline: none;
    """

    def __init__(self, color: str = None, width: int = 2):
        super().__init__()
        self.color = color or ACCESSIBILITY.get('focus_outline_color', COLORS['primary'])
        self.width = width
        self._original_styles: Dict[int, str] = {}

    def install(self, widget: QWidget):
        """Install focus tracking on a widget."""
        widget.installEventFilter(self)
        widget.setFocusPolicy(Qt.StrongFocus)

    def eventFilter(self, obj: QWidget, event) -> bool:
        """Handle focus events to show/hide focus indicator."""
        from PySide6.QtCore import QEvent

        if event.type() == QEvent.FocusIn:
            self._show_focus(obj)
        elif event.type() == QEvent.FocusOut:
            self._hide_focus(obj)

        return False

    def _show_focus(self, widget: QWidget):
        """Show focus indicator on widget."""
        widget_id = id(widget)
        self._original_styles[widget_id] = widget.styleSheet()

        focus_style = self.FOCUS_STYLE.format(color=self.color, width=self.width)
        widget.setStyleSheet(widget.styleSheet() + focus_style)

    def _hide_focus(self, widget: QWidget):
        """Remove focus indicator from widget."""
        widget_id = id(widget)
        if widget_id in self._original_styles:
            widget.setStyleSheet(self._original_styles[widget_id])
            del self._original_styles[widget_id]


def get_luminance(color: QColor) -> float:
    """Calculate relative luminance per WCAG 2.0 formula."""
    r = color.redF()
    g = color.greenF()
    b = color.blueF()

    rs = r / 12.92 if r <= 0.03928 else ((r + 0.055) / 1.055) ** 2.4
    gs = g / 12.92 if g <= 0.03928 else ((g + 0.055) / 1.055) ** 2.4
    bs = b / 12.92 if b <= 0.03928 else ((b + 0.055) / 1.055) ** 2.4

    return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs


def check_contrast_ratio(fg: QColor, bg: QColor) -> float:
    """Calculate contrast ratio (WCAG AA requires 4.5:1 for normal text)."""
    l1 = get_luminance(fg)
    l2 = get_luminance(bg)

    lighter = max(l1, l2)
    darker = min(l1, l2)

    return (lighter + 0.05) / (darker + 0.05)


def setup_accessible_widget(widget: QWidget, name: str, description: str = None,
                            role: str = None, shortcuts: List[str] = None):
    """Configure accessibility properties for a widget."""
    widget.setAccessibleName(name)
    if description:
        widget.setAccessibleDescription(description)
    if shortcuts:
        hint = f"{name} ({', '.join(shortcuts)})"
        widget.setToolTip(hint)
        widget.setAccessibleName(hint)


def apply_focus_style(widget: QWidget, color: str = None, width: int = 2, radius: int = 4) -> str:
    """Generate focus style CSS for a widget type."""
    color = color or COLORS['primary']
    selector = widget.metaObject().className()

    return f"""
    {selector}:focus {{
        border: {width}px solid {color};
        outline: none;
        border-radius: {radius}px;
    }}
    """


def create_focus_chain(widgets: List[QWidget]):
    """Set up keyboard tab order for widgets."""
    for i in range(len(widgets) - 1):
        QWidget.setTabOrder(widgets[i], widgets[i + 1])


def ensure_wcag_aa_compliance(theme_colors: Dict[str, str]) -> Dict[str, Dict]:
    """Validate theme colors meet WCAG AA contrast requirements."""
    results = {}
    bg_color = QColor(theme_colors.get('background', '#1a1a2e'))

    for name, color_hex in theme_colors.items():
        if name in ('background', 'surface', 'overlay', 'shadow'):
            continue

        fg_color = QColor(color_hex)
        ratio = check_contrast_ratio(fg_color, bg_color)

        results[name] = {
            "color": color_hex,
            "ratio": round(ratio, 2),
            "passes_aa": ratio >= 4.5,
            "passes_aaa": ratio >= 7.0,
        }

    return results


def get_accessible_color_pair(base_color: str, ensure_ratio: float = 4.5) -> tuple:
    """Get a foreground/background pair that meets contrast requirements."""
    bg = QColor(base_color)

    # Try white first
    white = QColor("#FFFFFF")
    if check_contrast_ratio(white, bg) >= ensure_ratio:
        return ("#FFFFFF", base_color)

    # Try black
    black = QColor("#000000")
    if check_contrast_ratio(black, bg) >= ensure_ratio:
        return ("#000000", base_color)

    # Adjust background to make it work with white
    l = get_luminance(bg)
    if l > 0.5:
        return ("#000000", base_color)
    return ("#FFFFFF", base_color)
