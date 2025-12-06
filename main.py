#!/usr/bin/env python3
"""
main.py - Application entry point for v3.0
"""

import os
import sys
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QPalette, QColor
from PySide6.QtCore import Qt
from app_gui import MainWindow

# Embedded secret salt (will be obfuscated in compiled EXE)
SECRET_SALT = "DTSEN_PRODUCTION_SALT_2024_V2_SECURE_"
os.environ["APP_SECRET_SALT"] = SECRET_SALT

def main():
    """Main application entry"""
    # Create required directories
    os.makedirs("output", exist_ok=True)
    os.makedirs("config", exist_ok=True)

    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    # Initialize Qt Application
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Set dark palette
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(30, 30, 46))
    palette.setColor(QPalette.WindowText, Qt.white)
    palette.setColor(QPalette.Base, QColor(24, 24, 36))
    palette.setColor(QPalette.AlternateBase, QColor(30, 30, 46))
    palette.setColor(QPalette.Text, Qt.white)
    palette.setColor(QPalette.Button, QColor(30, 30, 46))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    app.setPalette(palette)

    app.setApplicationName("DTSEN Scraper Pro")
    app.setOrganizationName("DTSEN Solutions")

    # Create and show main window
    window = MainWindow()
    window.show()

    # Start event loop
    sys.exit(app.exec())

if __name__ == "__main__":
    main()