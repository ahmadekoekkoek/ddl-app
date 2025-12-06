"""
GUI Widgets Module
Custom widgets: CircularProgress, StepIndicator, ModernButton
"""

from PySide6.QtWidgets import QWidget, QPushButton
from PySide6.QtCore import Qt, QPoint
from PySide6.QtGui import QPainter, QColor, QFont

from .constants import COLORS, FONTS


class CircularProgress(QWidget):
    """Circular progress indicator"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(120, 120)
        self._value = 0
        self._maximum = 100

    def setValue(self, value):
        self._value = value
        self.update()

    def setMaximum(self, maximum):
        self._maximum = maximum
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Background circle
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor(40, 40, 60))
        painter.drawEllipse(10, 10, 100, 100)

        # Progress arc
        if self._maximum > 0:
            angle = int(360 * (self._value / self._maximum) * 16)
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(COLORS['primary']))
            painter.drawPie(10, 10, 100, 100, 90 * 16, -angle)

        # Inner circle
        painter.setBrush(QColor(30, 30, 46))
        painter.drawEllipse(20, 20, 80, 80)

        # Text
        painter.setPen(QColor(COLORS['text']))
        painter.setFont(QFont(FONTS['family'], 16, QFont.Bold))
        if self._maximum > 0:
            percentage = int((self._value / self._maximum) * 100)
            painter.drawText(self.rect(), Qt.AlignCenter, f"{percentage}%")


class StepIndicator(QWidget):
    """Figma-style connecting dot step indicator with step names"""

    def __init__(self, steps, parent=None):
        super().__init__(parent)
        self.steps = steps
        self.current_step = 0
        self.setMinimumHeight(80)  # Increased to accommodate labels

    def set_step(self, index):
        if 0 <= index < len(self.steps):
            self.current_step = index
            self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        padding_x = 40
        available_width = self.width() - (padding_x * 2)
        step_width = available_width / (len(self.steps) - 1) if len(self.steps) > 1 else 0

        center_y = 25  # Moved up to make room for labels

        # Draw connecting lines
        painter.setPen(Qt.NoPen)
        for i in range(len(self.steps) - 1):
            start_x = padding_x + (i * step_width)
            end_x = padding_x + ((i + 1) * step_width)

            # Line color based on progress
            if i < self.current_step:
                color = QColor(COLORS['success'])  # Completed
            else:
                color = QColor(50, 50, 70)  # Incomplete

            painter.setBrush(color)
            painter.drawRect(int(start_x), center_y - 2, int(step_width), 4)

        # Draw dots
        for i in range(len(self.steps)):
            cx = padding_x + (i * step_width)

            # Dot color/size
            if i == self.current_step:
                # Active
                size = 24
                color = QColor(COLORS['primary'])
                glow_color = QColor(COLORS['primary'])
                glow_color.setAlpha(100)
                # Draw glow
                painter.setBrush(glow_color)
                painter.drawEllipse(QPoint(int(cx), center_y), size//2 + 4, size//2 + 4)
            elif i < self.current_step:
                # Completed
                size = 16
                color = QColor(COLORS['success'])
            else:
                # Pending
                size = 12
                color = QColor(50, 50, 70)

            painter.setBrush(color)
            painter.drawEllipse(QPoint(int(cx), center_y), size//2, size//2)

        # Draw step names below dots (only for current and adjacent steps)
        painter.setFont(QFont(FONTS['family'], 11))
        for i in range(len(self.steps)):
            cx = padding_x + (i * step_width)

            # Show label for current step and adjacent steps
            if abs(i - self.current_step) <= 1:
                # Determine text color
                if i == self.current_step:
                    text_color = QColor(COLORS['primary'])
                    painter.setFont(QFont(FONTS['family'], 11, QFont.Bold))
                elif i < self.current_step:
                    text_color = QColor(COLORS['success'])
                    painter.setFont(QFont(FONTS['family'], 11))
                else:
                    text_color = QColor(COLORS['text'])
                    painter.setFont(QFont(FONTS['family'], 11))

                painter.setPen(text_color)

                # Draw text below the dot
                text_rect = painter.boundingRect(0, 0, 200, 30, Qt.AlignCenter, self.steps[i])
                text_x = int(cx - text_rect.width() / 2)
                text_y = center_y + 20

                painter.drawText(text_x, text_y, text_rect.width(), text_rect.height(),
                               Qt.AlignCenter, self.steps[i])


class ModernButton(QPushButton):
    """Button with hover effect"""

    def __init__(self, text, primary=False, danger=False):
        super().__init__(text)
        self.primary = primary
        self.danger = danger
        self.update_style()

    def update_style(self):
        if self.danger:
            base_color = COLORS['error']
            hover_color = "#e74c3c"
        elif self.primary:
            base_color = COLORS['secondary']
            hover_color = COLORS['secondary_hover']
        else:
            base_color = COLORS['primary']
            hover_color = COLORS['primary_hover']

        self.setStyleSheet(f"""
            QPushButton {{
                background-color: {base_color};
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 8px;
                font-weight: bold;
                font-size: 14px;
                min-width: 120px;
                font-family: "{FONTS['family']}", sans-serif;
            }}
            QPushButton:hover {{
                background-color: {hover_color};
            }}
            QPushButton:pressed {{
                background-color: {base_color};
            }}
            QPushButton:disabled {{
                background-color: #7f8c8d;
            }}
            QPushButton:focus {{
                border: 2px solid #ffffff;
                outline: none;
            }}
        """)
        self.setAccessibleName(self.text())
