"""
GUI Widgets Module
Custom widgets: CircularProgress, StepIndicator, ModernButton, PackageCard
"""

from PySide6.QtWidgets import QWidget, QPushButton, QFrame, QLabel, QVBoxLayout, QHBoxLayout, QGraphicsDropShadowEffect
from PySide6.QtCore import Qt, QPoint, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QPainter, QColor, QFont

from .constants import COLORS, FONTS


class PackageCard(QFrame):
    """A visually elegant and informative card for package selection."""
    def __init__(self, package_info: dict):
        super().__init__()
        self.package_info = package_info
        self.setFixedWidth(280)
        self.setMinimumHeight(380)
        self.setup_ui()

    def setup_ui(self):
        color = self.package_info.get("color", "#3498db")
        is_rec = self.package_info.get("recommended", False)

        border_style = f"border: 2px solid {color};" if is_rec else "border: 1px solid #3a3a5e;"
        bg_style = f"""
            QFrame {{
                background-color: #2c3e50;
                border-radius: 15px;
                {border_style}
            }}
            QFrame:hover {{
                background-color: #34495e;
                border: 2px solid {color};
            }}
        """
        self.setStyleSheet(bg_style)
        self.setCursor(Qt.PointingHandCursor)

        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(25)
        shadow.setColor(QColor(0, 0, 0, 90))
        shadow.setOffset(0, 5)
        self.setGraphicsEffect(shadow)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(12)

        if is_rec:
            badge = QLabel("★ REKOMENDASI ★")
            badge.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 12px; letter-spacing: 1px; background: transparent; border: none;")
            badge.setAlignment(Qt.AlignCenter)
            layout.addWidget(badge)

        name_label = QLabel(self.package_info["name"])
        name_label.setStyleSheet("font-size: 24px; font-weight: 800; color: white; background: transparent; border: none;")
        name_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(name_label)

        self.price_label = QLabel(self.package_info["price"])
        self.price_label.setStyleSheet(f"font-size: 28px; font-weight: bold; color: {color}; margin: 5px 0; background: transparent; border: none;")
        self.price_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.price_label)

        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #3a3a5e;")
        layout.addWidget(line)

        for feature in self.package_info["features"]:
            feat_layout = QHBoxLayout()
            check = QLabel("✓")
            check.setStyleSheet(f"color: {color}; font-weight: bold; background: transparent; border: none;")
            text = QLabel(feature)
            text.setStyleSheet("color: #bdc3c7; font-size: 13px; background: transparent; border: none;")
            text.setWordWrap(True)
            feat_layout.addWidget(check)
            feat_layout.addWidget(text, 1)
            feat_layout.setAlignment(Qt.AlignTop)
            layout.addLayout(feat_layout)

        layout.addStretch()

    def set_price(self, price: str):
        self.price_label.setText(price)


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


class CollapsibleFrame(QFrame):
    """A frame that can be collapsed and expanded with a smooth animation."""
    def __init__(self, title: str, parent: QWidget = None):
        super().__init__(parent)
        self.is_expanded = True
        self.setup_ui(title)

    def setup_ui(self, title: str):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.toggle_button = QPushButton(f"▼ {title}")
        self.toggle_button.setStyleSheet("""
            QPushButton {
                background-color: #34495e; color: white; border: none;
                padding: 10px; text-align: left; font-weight: bold;
            }
            QPushButton:hover { background-color: #4a6fa5; }
        """)
        self.toggle_button.clicked.connect(self.toggle)

        self.content_frame = QFrame()
        self.content_frame.setStyleSheet("background-color: #2c3e50; border-top: 1px solid #34495e;")
        self.content_layout = QVBoxLayout(self.content_frame)
        self.content_layout.setContentsMargins(10, 10, 10, 10)

        main_layout.addWidget(self.toggle_button)
        main_layout.addWidget(self.content_frame)

        # Animation setup
        self.animation = QPropertyAnimation(self.content_frame, b"maximumHeight")
        self.animation.setDuration(300)
        self.animation.setEasingCurve(QEasingCurve.InOutQuad)

    def setContentLayout(self, layout):
        # Clear existing layout and repopulate
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self.content_layout.addLayout(layout)

        # Ensure the frame starts with the correct size
        self.content_frame.adjustSize()
        self.toggle(instant=True)

    def toggle(self, instant=False):
        self.is_expanded = not self.is_expanded
        arrow = "▼" if self.is_expanded else "►"
        self.toggle_button.setText(f"{arrow} {self.toggle_button.text().split(' ', 1)[1]}")

        start_height = self.content_frame.height()
        end_height = self.content_frame.sizeHint().height() if self.is_expanded else 0

        if instant:
            self.content_frame.setMaximumHeight(end_height)
        else:
            self.animation.setStartValue(start_height)
            self.animation.setEndValue(end_height)
            self.animation.start()
