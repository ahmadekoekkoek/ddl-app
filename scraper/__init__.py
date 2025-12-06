"""
Scraper Package - Modular architecture for DTSEN data scraping and reporting.

Components:
- APIClient: HTTP requests, encryption, authentication
- DataProcessor: Data cleaning, merging, transformations
- ReportGenerator: Excel workbook creation
- PDFBuilder: PDF report generation
- Visualizer: Chart creation with fallbacks
- ProgressTracker: Progress callbacks and ETA
- ScraperFacade: Orchestrates all components
"""

from .api_client import APIClient
from .data_processor import DataProcessor
from .report_generator import ReportGenerator
from .pdf_builder import PDFBuilder
from .visualizer import Visualizer
from .progress_tracker import ProgressTracker
from .facade import ScraperFacade
from .constants import (
    URL_FAMILY, URL_MEMBERS, URL_KYC, URL_PBI, URL_PKH, URL_BPNT,
    URL_ASET, URL_ASET_BERGERAK, ENDPOINTS,
    FAMILY_HEADERS, MEMBER_HEADERS, ASSET_IMMOVABLE, ASSET_MOVABLE, ASSET_ALIASES,
    COLS_TO_DROP, TEXT_COLS, DATE_COLS, DESIL_LABELS
)

__all__ = [
    # Classes
    'APIClient',
    'DataProcessor',
    'ReportGenerator',
    'PDFBuilder',
    'Visualizer',
    'ProgressTracker',
    'ScraperFacade',
    # Constants
    'URL_FAMILY', 'URL_MEMBERS', 'URL_KYC', 'URL_PBI', 'URL_PKH', 'URL_BPNT',
    'URL_ASET', 'URL_ASET_BERGERAK', 'ENDPOINTS',
    'FAMILY_HEADERS', 'MEMBER_HEADERS', 'ASSET_IMMOVABLE', 'ASSET_MOVABLE', 'ASSET_ALIASES',
    'COLS_TO_DROP', 'TEXT_COLS', 'DATE_COLS', 'DESIL_LABELS',
]
