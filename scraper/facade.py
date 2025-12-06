"""
Scraper Facade
Orchestrates all scraper components into a unified interface.
"""

import io
import secrets
import asyncio
from typing import Dict, List, Any, Callable, Optional

import pandas as pd

from core import (
    get_logger, LogContext, MemoryMonitor,
    validate_entity_lines, cleanup_resources
)
from .constants import URL_FAMILY, ENDPOINTS
from .api_client import APIClient
from .data_processor import DataProcessor
from .report_generator import ReportGenerator
from .pdf_builder import PDFBuilder
from .visualizer import Visualizer
from .progress_tracker import ProgressTracker

# Check for httpx
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class ScraperFacade:
    """
    Main orchestrator that coordinates all scraper components.

    Maintains backward compatibility with InMemoryScraper while
    using the new modular architecture internally.
    """

    def __init__(self, bearer_token: str, aes_key_b64: str,
                 progress_callback: Callable[[str, int, int], None] = None):
        self._logger = get_logger('scraper_facade')

        # Initialize components
        self.api_client = APIClient(bearer_token, aes_key_b64)
        self.data_processor = DataProcessor()
        self.visualizer = Visualizer()
        self.report_generator = ReportGenerator(self.data_processor, self.visualizer)
        self.pdf_builder = PDFBuilder(self.data_processor, self.visualizer)
        self.progress_tracker = ProgressTracker(progress_callback)

        # Memory monitoring
        self._memory_monitor = MemoryMonitor(warning_threshold_mb=300)

        # Store credentials for backward compatibility
        self.token = bearer_token
        self.aes_key = aes_key_b64

    def _update_progress(self, stage: str, current: int, total: int):
        """Update progress via tracker."""
        self.progress_tracker.update(stage, current, total)

    @validate_entity_lines(min_length=1, max_length=10000)
    def run_full_pipeline(self, entity_lines: List[str],
                          pre_captured_families: List[Dict] = None) -> Dict[str, Any]:
        """
        Execute the complete scraping and report generation pipeline.

        Args:
            entity_lines: List of encrypted entity payload strings
            pre_captured_families: Optional pre-captured family data to skip API fetch
        """
        tx_id = f"TX-{secrets.token_hex(5).upper()}"
        LogContext.set(transaction_id=tx_id, entity_count=len(entity_lines))

        self._logger.info(f"Starting pipeline {tx_id} with {len(entity_lines)} entities")
        self._memory_monitor.set_baseline()
        self.progress_tracker.start(len(entity_lines))

        try:
            files_dict = {}

            # Step 1: Use pre-captured families OR fetch from API
            if pre_captured_families:
                self._logger.info(f"Using {len(pre_captured_families)} pre-captured families (skipping API fetch)")
                families = pre_captured_families
                # Add backward compatibility for id_keluarga_parent
                for row in families:
                    if "id_keluarga_parent" not in row or not row["id_keluarga_parent"]:
                        idk = row.get("id_keluarga") or row.get("ID_KELUARGA")
                        row["id_keluarga_parent"] = idk
            else:
                self._logger.info("Fetching families from API...")
                families = self.api_client.fetch_families(
                    URL_FAMILY,
                    entity_lines,
                    progress_cb=self._update_progress
                )

            if not families:
                raise ValueError("No families fetched")

            # Save families CSV
            families_df = pd.DataFrame(families)
            files_dict["families_raw.csv"] = families_df.to_csv(index=False).encode()

            # Extract family IDs
            id_list = [
                f.get("id_keluarga_parent") or f.get("id_keluarga") or f.get("ID_KELUARGA")
                for f in families
            ]
            id_list = [i for i in id_list if i]

            # Step 2: Fetch all endpoints
            self._logger.info("Fetching endpoint data...")

            if HTTPX_AVAILABLE:
                try:
                    files_dict = self._fetch_endpoints_async(id_list, files_dict)
                except Exception as e:
                    self._logger.warning(f"Async fetch failed: {e}, using sync")
                    files_dict = self._fetch_endpoints_sync(id_list, files_dict)
            else:
                files_dict = self._fetch_endpoints_sync(id_list, files_dict)

            # Step 3: Process raw data
            self._logger.info("Processing raw data...")
            files_dict = self.data_processor.process_raw_data(files_dict)

            # Step 4: Build master sheets
            self._logger.info("Building master sheets...")
            keluarga_master = self.data_processor.build_keluarga_master(files_dict)
            anggota_master = self.data_processor.build_anggota_master(files_dict)
            desil_sheets = self.data_processor.build_desil_sheets(keluarga_master)

            # Step 5: Create visualizations
            self._logger.info("Creating visualizations...")
            charts = self.visualizer.create_visualizations(
                keluarga_master, anggota_master, files_dict
            )

            # Step 6: Build XLSX with fallback
            self._update_progress("[DATA] Building XLSX", 0, 100)
            try:
                xlsx_bytes = self.report_generator.build_xlsx(
                    files_dict, keluarga_master, anggota_master, desil_sheets, charts
                )
                files_dict["Rekapitulasi.xlsx"] = xlsx_bytes
                self.progress_tracker.xlsx_generated("Rekapitulasi.xlsx")
                self._logger.info("XLSX generation successful")
            except Exception as e:
                self._logger.error(f"XLSX failed: {e}, using CSV fallback")
                csv_data, _ = self.report_generator._build_fallback_csv(
                    files_dict, keluarga_master, anggota_master
                )
                files_dict["Rekapitulasi_fallback.zip"] = csv_data

            # Step 7: Build PDFs with error handling
            self._update_progress("[PDF] Building PDFs", 0, 100)
            try:
                pdf_files = self.pdf_builder.build_pdfs(
                    files_dict, keluarga_master, anggota_master, charts
                )
                files_dict.update(pdf_files)
                for pdf_name in pdf_files.keys():
                    self.progress_tracker.pdf_generated(pdf_name)
                self._logger.info(f"Generated {len(pdf_files)} PDF files")
            except Exception as e:
                self._logger.error(f"PDF generation failed: {e}")

            # Complete
            self._memory_monitor.log_usage("pipeline_end")
            mem_report = self._memory_monitor.get_report()
            progress_summary = self.progress_tracker.complete()

            self._logger.info(
                f"Pipeline complete - Memory: {mem_report['current_mb']:.1f}MB "
                f"(peak: {mem_report['peak_mb']:.1f}MB)"
            )

            return {
                "tx_id": tx_id,
                "files": files_dict,
                "memory_report": mem_report,
                "progress_summary": progress_summary
            }

        finally:
            self.api_client.close()
            self.visualizer.cleanup()
            LogContext.clear()

    def _fetch_endpoints_sync(self, id_list: List[str],
                               files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """Fetch all endpoints synchronously."""
        for label, url in ENDPOINTS.items():
            self._update_progress(f"[FETCH] Fetching {label}", 0, len(id_list))

            data = self.api_client.fetch_endpoint(
                label, url, id_list,
                progress_cb=self._update_progress
            )

            df = pd.DataFrame(data) if data else pd.DataFrame(columns=["id_keluarga_parent"])
            files_dict[f"{label}_raw.csv"] = df.to_csv(index=False).encode()

        return files_dict

    def _fetch_endpoints_async(self, id_list: List[str],
                                files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """Fetch all endpoints asynchronously."""
        self._logger.info("Starting concurrent scraping for all endpoints...")

        async def fetch_all():
            tasks = []
            labels = []

            for label, url in ENDPOINTS.items():
                labels.append(label)
                tasks.append(self.api_client.fetch_endpoint_async(
                    label, url, id_list,
                    progress_cb=self._update_progress
                ))

            results = await asyncio.gather(*tasks)
            return dict(zip(labels, results))

        results_map = asyncio.run(fetch_all())

        for label, data in results_map.items():
            df = pd.DataFrame(data) if data else pd.DataFrame(columns=["id_keluarga_parent"])
            files_dict[f"{label}_raw.csv"] = df.to_csv(index=False).encode()

        return files_dict

    # Backward compatibility methods

    def fetch_families(self, entity_lines: List[str]) -> List[Dict]:
        """Backward compatible method."""
        return self.api_client.fetch_families(
            URL_FAMILY, entity_lines,
            progress_cb=self._update_progress
        )

    def fetch_endpoint(self, label: str, url: str, id_list: List[str]) -> List[Dict]:
        """Backward compatible method."""
        return self.api_client.fetch_endpoint(
            label, url, id_list,
            progress_cb=self._update_progress
        )

    def build_keluarga_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Backward compatible method."""
        return self.data_processor.build_keluarga_master(files_dict)

    def build_anggota_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Backward compatible method."""
        return self.data_processor.build_anggota_master(files_dict)

    def build_xlsx(self, files_dict: Dict[str, bytes]) -> bytes:
        """Backward compatible method."""
        keluarga = self.data_processor.build_keluarga_master(files_dict)
        anggota = self.data_processor.build_anggota_master(files_dict)
        desil = self.data_processor.build_desil_sheets(keluarga)
        charts = self.visualizer.create_visualizations(keluarga, anggota, files_dict)
        return self.report_generator.build_xlsx(files_dict, keluarga, anggota, desil, charts)

    def build_pdfs(self, files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """Backward compatible method."""
        keluarga = self.data_processor.build_keluarga_master(files_dict)
        anggota = self.data_processor.build_anggota_master(files_dict)
        charts = self.visualizer.create_visualizations(keluarga, anggota, files_dict)
        return self.pdf_builder.build_pdfs(files_dict, keluarga, anggota, charts)

    def create_visualizations(self, files_dict: Dict[str, bytes],
                              keluarga_master: pd.DataFrame = None,
                              anggota_master: pd.DataFrame = None) -> Dict[str, bytes]:
        """Backward compatible method."""
        if keluarga_master is None:
            keluarga_master = self.data_processor.build_keluarga_master(files_dict)
        if anggota_master is None:
            anggota_master = self.data_processor.build_anggota_master(files_dict)
        return self.visualizer.create_visualizations(keluarga_master, anggota_master, files_dict)

    def clean_aset(self, raw: List[Dict]) -> pd.DataFrame:
        """Backward compatible method."""
        return self.data_processor.clean_aset(raw)

    def clean_aset_bergerak(self, raw: List[Dict]) -> pd.DataFrame:
        """Backward compatible method."""
        return self.data_processor.clean_aset_bergerak(raw)
