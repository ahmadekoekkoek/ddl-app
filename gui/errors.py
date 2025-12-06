"""
GUI Error Handling Module
Structured error taxonomy and recovery strategies
"""

class AppError(Exception):
    """Base class for all application errors"""

    def __init__(self, message, original_error=None, context=None):
        """
        Args:
            message: User-friendly error message
            original_error: The underlying exception (if any)
            context: Dictionary with additional context (e.g., variable values)
        """
        super().__init__(message)
        self.message = message
        self.original_error = original_error
        self.context = context or {}

    def __str__(self):
        if self.original_error:
            return f"{self.message} (Caused by: {str(self.original_error)})"
        return self.message


class ConfigError(AppError):
    """Configuration validation errors"""
    pass


class NetworkError(AppError):
    """Network connectivity and API errors"""

    def __init__(self, message, original_error=None, context=None, status_code=None):
        super().__init__(message, original_error, context)
        self.status_code = status_code


class PaymentError(AppError):
    """Payment processing errors"""
    pass


class ScrapingError(AppError):
    """Scraping process errors"""
    pass


class DecryptionError(AppError):
    """Data decryption errors"""
    pass


def format_error_message(error: AppError) -> str:
    """
    Format error for display to user

    Returns:
        HTML formatted string with title and details
    """
    title = "Terjadi Kesalahan"
    icon = "❌"

    if isinstance(error, ConfigError):
        title = "Kesalahan Konfigurasi"
        icon = "⚙️"
    elif isinstance(error, NetworkError):
        title = "Masalah Koneksi"
        icon = "🌐"
    elif isinstance(error, PaymentError):
        title = "Gagal Pembayaran"
        icon = "💳"
    elif isinstance(error, ScrapingError):
        title = "Gagal Scraping"
        icon = "🕷️"

    html = f"""
    <h3>{icon} {title}</h3>
    <p style='font-size: 14px; color: #e74c3c;'>{error.message}</p>
    """

    if error.context:
        html += "<p style='font-size: 12px; color: #95a5a6;'>Detail:</p><ul>"
        for k, v in error.context.items():
            html += f"<li><b>{k}:</b> {v}</li>"
        html += "</ul>"

    return html


class RetryHandler:
    """
    Helper for retrying operations with exponential backoff
    """

    def __init__(self, max_retries=3, base_delay=1.0, max_delay=10.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def execute(self, func, *args, **kwargs):
        """
        Execute function with retry logic

        Args:
            func: Function to execute
            *args, **kwargs: Arguments for function

        Returns:
            Result of function call

        Raises:
            Last exception encountered if all retries fail
        """
        import time
        import random

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if attempt < self.max_retries:
                    # Calculate delay with jitter
                    delay = min(self.max_delay, self.base_delay * (2 ** attempt))
                    jitter = delay * 0.1 * random.random()
                    sleep_time = delay + jitter

                    print(f"[RetryHandler] Attempt {attempt+1} failed: {e}. Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else:
                    print(f"[RetryHandler] All {self.max_retries+1} attempts failed.")

        if last_exception:
            raise last_exception


def show_error_dialog(parent, error_message, title="Error"):
    """
    Show a user-friendly error dialog

    Args:
        parent: Parent widget
        error_message: Error message or AppError instance
        title: Dialog title
    """
    from PySide6.QtWidgets import QMessageBox
    from PySide6.QtCore import Qt

    msg = QMessageBox(parent)
    msg.setWindowTitle(title)
    msg.setIcon(QMessageBox.Critical)

    if isinstance(error_message, AppError):
        # Use formatted HTML for AppError
        msg.setTextFormat(Qt.RichText)
        msg.setText(format_error_message(error_message))
    else:
        # Standard text for string errors
        msg.setText(str(error_message))

    msg.setStandardButtons(QMessageBox.Ok)
    msg.exec()
