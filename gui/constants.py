"""
GUI Constants Module
Centralized colors, fonts, strings, animation parameters, and layout constants
"""

from PySide6.QtGui import QFont

# ============================================================================
# COLOR PALETTE
# ============================================================================

COLORS = {
    # Primary colors
    'primary': '#2F75B5',
    'primary_hover': '#3d8fd1',
    'secondary': '#27ae60',
    'secondary_hover': '#2ecc71',

    # Background colors
    'background': '#1a1a2e',
    'surface': '#252538',
    'surface_hover': '#2d2d42',

    # Text colors
    'text': '#ecf0f1',
    'text_secondary': '#95a5a6',
    'text_disabled': '#5a5a6e',

    # Status colors
    'success': '#27ae60',
    'error': '#e74c3c',
    'warning': '#f39c12',
    'info': '#3498db',

    # UI element colors
    'border': '#34344a',
    'shadow': 'rgba(0, 0, 0, 0.3)',
    'overlay': 'rgba(0, 0, 0, 0.5)',
}

# High-contrast mode palette
HIGH_CONTRAST_COLORS = {
    'background': '#000000',
    'text': '#FFFFFF',
    'primary': '#00FFFF',
    'secondary': '#FFFF00',
    'error': '#FF0000',
    'success': '#00FF00',
    'warning': '#FFA500',
    'border': '#FFFFFF',
}

# ============================================================================
# TYPOGRAPHY
# ============================================================================

FONTS = {
    'family': 'Segoe UI',
    'family_mono': 'Consolas',

    # Font sizes
    'size_xs': 10,
    'size_sm': 11,
    'size_base': 12,
    'size_md': 13,
    'size_lg': 14,
    'size_xl': 16,
    'size_2xl': 18,
    'size_3xl': 24,
    'size_4xl': 32,

    # Font weights
    'weight_normal': QFont.Weight.Normal,
    'weight_medium': QFont.Weight.Medium,
    'weight_bold': QFont.Weight.Bold,
}

# ============================================================================
# LAYOUT CONSTANTS
# ============================================================================

SPACING = {
    'xs': 4,
    'sm': 8,
    'md': 16,
    'lg': 24,
    'xl': 32,
    'xxl': 48,
}

BORDER_RADIUS = {
    'small': 6,
    'medium': 8,
    'large': 12,
    'xlarge': 16,
}

SHADOWS = {
    'small': '0px 2px 4px rgba(0, 0, 0, 0.1)',
    'medium': '0px 4px 8px rgba(0, 0, 0, 0.15)',
    'large': '0px 8px 16px rgba(0, 0, 0, 0.2)',
    'xlarge': '0px 12px 24px rgba(0, 0, 0, 0.25)',
}

# ============================================================================
# ANIMATION PARAMETERS
# ============================================================================

ANIMATION = {
    # Durations (milliseconds)
    'duration_fast': 150,
    'duration_normal': 300,
    'duration_slow': 500,

    # Easing curves (Qt naming)
    'easing_default': 'OutCubic',
    'easing_smooth': 'InOutCubic',
    'easing_bounce': 'OutBounce',
    'easing_elastic': 'OutElastic',
}

# ============================================================================
# RESPONSIVE BREAKPOINTS
# ============================================================================

BREAKPOINTS = {
    'mobile': 768,
    'tablet': 1024,
    'desktop': 1280,
    'widescreen': 1920,
    'ultra': 2560,
}

# Base resolution for scaling calculations
BASE_RESOLUTION = {
    'width': 1280,
    'height': 720,
}

# ============================================================================
# INDONESIAN UI STRINGS
# ============================================================================

STRINGS = {
    # Stage titles
    'stage_config': '⚙️ Konfigurasi',
    'stage_confirm': '📋 Konfirmasi Data',
    'stage_processing': '🔄 Memproses',
    'stage_package': '📦 Pilih Paket',
    'stage_terms': '📜 Syarat & Ketentuan',
    'stage_payment': '💳 Pembayaran',
    'stage_success': '✅ Berhasil',

    # Button labels
    'btn_next': 'Lanjutkan',
    'btn_prev': 'Kembali',
    'btn_save': 'Simpan',
    'btn_validate': 'Validasi',
    'btn_cancel': 'Batal',
    'btn_confirm': 'Konfirmasi',
    'btn_skip': 'Lewati',
    'btn_retry': 'Coba Lagi',
    'btn_open_folder': 'Buka Folder',

    # Status messages
    'status_loading': 'Memuat...',
    'status_validating': 'Memvalidasi...',
    'status_processing': 'Memproses...',
    'status_complete': 'Selesai',
    'status_error': 'Terjadi Kesalahan',

    # Common labels
    'label_authorization': 'Authorization Token',
    'label_entity': 'Kode Entitas',
    'label_family_count': 'Jumlah Keluarga',
    'label_member_count': 'Jumlah Anggota',
    'label_output_folder': 'Folder Output',
    'label_progress': 'Progres',
    'label_speed': 'Kecepatan',
    'label_eta': 'Estimasi Waktu',

    # Error messages
    'error_network': 'Koneksi jaringan bermasalah',
    'error_token_expired': 'Token kedaluwarsa',
    'error_invalid_config': 'Konfigurasi tidak valid',
    'error_payment_failed': 'Pembayaran gagal',
    'error_scraping_failed': 'Scraping gagal',

    # Tooltips
    'tooltip_save': 'Simpan konfigurasi (Ctrl+S)',
    'tooltip_validate': 'Validasi konfigurasi (F9)',
    'tooltip_next': 'Lanjut ke tahap berikutnya (Ctrl+→)',
    'tooltip_prev': 'Kembali ke tahap sebelumnya (Ctrl+←)',
}

# ============================================================================
# PERFORMANCE THRESHOLDS
# ============================================================================

PERFORMANCE = {
    'max_memory_mb': 300,
    'target_fps': 60,
    'max_stage_load_ms': 500,
    'animation_throttle_ms': 16,  # ~60 FPS
}

# ============================================================================
# ACCESSIBILITY
# ============================================================================

ACCESSIBILITY = {
    'min_contrast_ratio': 4.5,  # WCAG 2.1 AA
    'focus_outline_width': 2,
    'focus_outline_color': COLORS['primary'],
}
