"""
GUI Module for DTSEN Downloader
Enterprise-grade modular architecture with accessibility, performance monitoring,
configuration profiles, audit logging, batch operations, and testing.
"""

from .constants import *
from .state import AppState
from .widgets import CircularProgress, StepIndicator, ModernButton
from .workers import ScraperWorker, ConfigValidationWorker
from .stages import (
    StageWidget, ConfigStage, PreScrapeConfirmStage,
    ProcessingStage, PackageStage, TermsStage,
    PaymentStage, SuccessStage
)
from .animations import AnimationManager, AnimationController
from .thread_pool import ThreadPoolManager
from .lazy_loader import LazyWidgetLoader, LoadingSkeleton
from .responsive import ResponsiveScaler
from .flow_layout import FlowLayout
from .responsive_container import ResponsiveContainer
from .micro_interactions import (
    HoverEffect, PressEffect, GlowEffect, RippleEffect, LoadingMorph,
    apply_hover_effect, apply_press_effect, apply_glow_effect
)
from .errors import (
    AppError, ConfigError, NetworkError, PaymentError,
    ScrapingError, DecryptionError, format_error_message,
    RetryHandler, show_error_dialog
)
from .accessibility import (
    AccessibilityManager, KeyboardShortcutManager, FocusIndicator,
    check_contrast_ratio, setup_accessible_widget, create_focus_chain,
    ensure_wcag_aa_compliance, get_accessible_color_pair
)
from .performance_monitor import (
    PerformanceMonitor, PerformanceMetrics, PerformanceGrade,
    StageMetrics, OptimizationRecommendation,
    get_performance_monitor
)
from .config_profiles import (
    ConfigProfile, ProfileManager, ProfileEncryption,
    EnvironmentManager, get_profile_manager
)
from .audit_logger import (
    AuditLogger, AuditEntry, AuditEventType, AuditSeverity,
    get_audit_logger
)
from .batch_operations import (
    BatchJob, BatchQueue, BatchJobBuilder,
    JobPriority, JobStatus, get_batch_queue
)
from .gui_test_suite import (
    GUITestSuite, GUITestCase, TestResult, TestSuiteResult,
    PerformanceBenchmark, ResponsiveLayoutTest, ScreenshotCapture
)
from .whatsapp_payment_stage import (
    WhatsAppPaymentStage, WhatsAppOrderManager, WhatsAppOrder,
    OrderStatus, SELLER_WHATSAPP_DISPLAY
)

__all__ = [
    # State management
    'AppState',

    # Widgets
    'CircularProgress',
    'StepIndicator',
    'ModernButton',

    # Workers
    'ScraperWorker',
    'ConfigValidationWorker',

    # Stages
    'StageWidget',
    'ConfigStage',
    'PreScrapeConfirmStage',
    'ProcessingStage',
    'PackageStage',
    'TermsStage',
    'PaymentStage',
    'SuccessStage',

    # WhatsApp Payment
    'WhatsAppPaymentStage',
    'WhatsAppOrderManager',
    'WhatsAppOrder',
    'OrderStatus',
    'SELLER_WHATSAPP_DISPLAY',

    # Animations
    'AnimationManager',
    'AnimationController',

    # Performance
    'ThreadPoolManager',
    'LazyWidgetLoader',
    'LoadingSkeleton',

    # Responsive
    'ResponsiveScaler',
    'FlowLayout',
    'ResponsiveContainer',

    # Micro-interactions
    'HoverEffect',
    'PressEffect',
    'GlowEffect',
    'RippleEffect',
    'LoadingMorph',
    'apply_hover_effect',
    'apply_press_effect',
    'apply_glow_effect',

    # Errors
    'AppError',
    'ConfigError',
    'NetworkError',
    'PaymentError',
    'ScrapingError',
    'DecryptionError',
    'format_error_message',
    'RetryHandler',
    'show_error_dialog',

    # Accessibility
    'AccessibilityManager',
    'KeyboardShortcutManager',
    'FocusIndicator',
    'check_contrast_ratio',
    'setup_accessible_widget',
    'create_focus_chain',
    'ensure_wcag_aa_compliance',
    'get_accessible_color_pair',

    # Performance Monitoring
    'PerformanceMonitor',
    'PerformanceMetrics',
    'PerformanceGrade',
    'StageMetrics',
    'OptimizationRecommendation',
    'get_performance_monitor',

    # Configuration Profiles
    'ConfigProfile',
    'ProfileManager',
    'ProfileEncryption',
    'EnvironmentManager',
    'get_profile_manager',

    # Audit Logging
    'AuditLogger',
    'AuditEntry',
    'AuditEventType',
    'AuditSeverity',
    'get_audit_logger',

    # Batch Operations
    'BatchJob',
    'BatchQueue',
    'BatchJobBuilder',
    'JobPriority',
    'JobStatus',
    'get_batch_queue',

    # GUI Testing
    'GUITestSuite',
    'GUITestCase',
    'TestResult',
    'TestSuiteResult',
    'PerformanceBenchmark',
    'ResponsiveLayoutTest',
    'ScreenshotCapture',

    # Constants (exported via *)
    'COLORS',
    'FONTS',
    'SPACING',
    'BORDER_RADIUS',
    'SHADOWS',
    'ANIMATION',
    'BREAKPOINTS',
    'STRINGS',
    'PERFORMANCE',
    'ACCESSIBILITY',
    'HIGH_CONTRAST_COLORS',
]
