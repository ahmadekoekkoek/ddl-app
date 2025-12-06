"""
GUI Flow Layout
Responsive flow layout that wraps widgets based on available space
"""

from PySide6.QtWidgets import QLayout, QWidgetItem, QSizePolicy
from PySide6.QtCore import Qt, QRect, QSize, QPoint


class FlowLayout(QLayout):
    """
    Flow layout that arranges widgets in rows, wrapping to next row when needed
    Perfect for responsive package cards, buttons, etc.
    """

    def __init__(self, parent=None, margin=0, h_spacing=10, v_spacing=10):
        super().__init__(parent)

        if parent is not None:
            self.setContentsMargins(margin, margin, margin, margin)

        self._h_spacing = h_spacing
        self._v_spacing = v_spacing
        self._item_list = []

    def __del__(self):
        item = self.takeAt(0)
        while item:
            item = self.takeAt(0)

    def addItem(self, item):
        self._item_list.append(item)

    def horizontalSpacing(self):
        if self._h_spacing >= 0:
            return self._h_spacing
        else:
            return self.smartSpacing(QStyle.PM_LayoutHorizontalSpacing)

    def verticalSpacing(self):
        if self._v_spacing >= 0:
            return self._v_spacing
        else:
            return self.smartSpacing(QStyle.PM_LayoutVerticalSpacing)

    def count(self):
        return len(self._item_list)

    def itemAt(self, index):
        if 0 <= index < len(self._item_list):
            return self._item_list[index]
        return None

    def takeAt(self, index):
        if 0 <= index < len(self._item_list):
            return self._item_list.pop(index)
        return None

    def expandingDirections(self):
        return Qt.Orientations(Qt.Orientation(0))

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        height = self._do_layout(QRect(0, 0, width, 0), True)
        return height

    def setGeometry(self, rect):
        super().setGeometry(rect)
        self._do_layout(rect, False)

    def sizeHint(self):
        return self.minimumSize()

    def minimumSize(self):
        size = QSize()

        for item in self._item_list:
            size = size.expandedTo(item.minimumSize())

        margin_left, margin_top, margin_right, margin_bottom = self.getContentsMargins()
        size += QSize(margin_left + margin_right, margin_top + margin_bottom)
        return size

    def _do_layout(self, rect, test_only):
        """
        Perform the layout calculation

        Args:
            rect: Rectangle to layout within
            test_only: If True, only calculate height without actually positioning

        Returns:
            int: Total height required
        """
        x = rect.x()
        y = rect.y()
        line_height = 0

        margin_left, margin_top, margin_right, margin_bottom = self.getContentsMargins()

        effective_rect = rect.adjusted(margin_left, margin_top, -margin_right, -margin_bottom)
        x = effective_rect.x()
        y = effective_rect.y()

        for item in self._item_list:
            widget = item.widget()
            space_x = self.horizontalSpacing()
            space_y = self.verticalSpacing()

            if space_x == -1:
                space_x = widget.style().layoutSpacing(
                    QSizePolicy.PushButton,
                    QSizePolicy.PushButton,
                    Qt.Horizontal
                )

            if space_y == -1:
                space_y = widget.style().layoutSpacing(
                    QSizePolicy.PushButton,
                    QSizePolicy.PushButton,
                    Qt.Vertical
                )

            next_x = x + item.sizeHint().width() + space_x

            if next_x - space_x > effective_rect.right() and line_height > 0:
                # Move to next line
                x = effective_rect.x()
                y = y + line_height + space_y
                next_x = x + item.sizeHint().width() + space_x
                line_height = 0

            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), item.sizeHint()))

            x = next_x
            line_height = max(line_height, item.sizeHint().height())

        return y + line_height - rect.y() + margin_bottom

    def smartSpacing(self, pm):
        """Get smart spacing from parent widget style"""
        parent = self.parent()
        if parent is None:
            return -1
        elif parent.isWidgetType():
            return parent.style().pixelMetric(pm, None, parent)
        else:
            return parent.spacing()
