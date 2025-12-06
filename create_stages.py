"""
Script to create gui/stages.py from extracted code
"""

# Read the extracted stages code
with open('gui/stages_temp.txt', 'r', encoding='utf-8') as f:
    stages_code = f.read()

# Create the header
header = '''"""
GUI Stages Module
All stage widget classes for the application workflow
"""

import os
import json
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit,
    QTextEdit, QPushButton, QFrame, QFileDialog, QSizePolicy, QMessageBox,
    QGraphicsDropShadowEffect, QScrollArea
)
from PySide6.QtCore import Qt, Signal, QUrl, QTimer
from PySide6.QtGui import QColor, QDesktopServices
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineSettings

from .widgets import CircularProgress, ModernButton
from .workers import ConfigValidationWorker
from system_info import get_system_specs, estimate_speed, get_default_output_folder, measure_network_metrics


'''

# Combine header and stages code
full_content = header + stages_code

# Write to gui/stages.py
with open('gui/stages.py', 'w', encoding='utf-8') as f:
    f.write(full_content)

print("Created gui/stages.py successfully!")
