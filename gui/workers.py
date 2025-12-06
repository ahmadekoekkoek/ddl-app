"""
GUI Workers Module
QThread worker classes for background tasks
"""

from PySide6.QtCore import QThread, Signal
from orchestrator import Orchestrator
from .errors import RetryHandler, NetworkError, DecryptionError, ConfigError
from scrape_and_build import decrypt_entity


class ScraperWorker(QThread):
    """Worker thread for scraping operations"""

    finished = Signal(dict)
    error = Signal(object)  # Changed to object to support AppError
    progress = Signal(str, int, int)

    def __init__(self, orchestrator: Orchestrator):
        super().__init__()
        self.orch = orchestrator

    def run(self):
        try:
            self.orch.start_process()
        except Exception as e:
            from .errors import ScrapingError, AppError
            if isinstance(e, AppError):
                self.error.emit(e)
            else:
                self.error.emit(ScrapingError(f"Scraping gagal: {str(e)}", original_error=e))


class ConfigValidationWorker(QThread):
    """Worker thread for validating config and counting families with deduplication"""

    validated = Signal(str, list, int)  # authorization, entities, total_unique_families
    families_extracted = Signal(list)   # Pre-captured families for optimized scraping
    status_update = Signal(str)
    error = Signal(str)

    def __init__(self, authorization: str, entities: list, aes_key: str):
        super().__init__()
        self.authorization = authorization
        self.entities = entities
        self.aes_key = aes_key
        self._collected_families = []  # Store families during validation

    def run(self):
        """Validate config, collect families, deduplicate, and count"""
        import requests
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from scraper.constants import URL_FAMILY
        from entity_deduplicator import deduplicate_families, get_deduplication_summary

        self.status_update.emit("🔍 Memvalidasi konfigurasi...")

        try:
            # Validate input
            if not self.authorization:
                raise ConfigError("Authorization token tidak boleh kosong")
            if not self.entities:
                raise ConfigError("Daftar entities tidak boleh kosong")
            if not self.aes_key:
                raise ConfigError("AES Key tidak ditemukan di .env")

            all_families = []  # Collect all raw families
            retry_handler = RetryHandler(max_retries=3, base_delay=1.0)

            headers = {
                'Authorization': self.authorization,
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Origin': 'https://siks.kemensos.go.id',
                'Referer': 'https://siks.kemensos.go.id/',
                'Accept': 'application/json, text/plain, */*'
            }

            # Helper function for single entity validation - now returns family rows
            def validate_single_entity(entity, index):
                if self.isInterruptionRequested():
                    return []

                payload = f"entity={entity}"
                try:
                    response = retry_handler.execute(
                        requests.post,
                        URL_FAMILY,
                        headers=headers,
                        data=payload,
                        timeout=30
                    )

                    if response.status_code == 200:
                        try:
                            resp_json = response.json()
                            enc = resp_json.get("data") if isinstance(resp_json, dict) else response.text.strip('"')

                            if not enc:
                                return []

                            dec = decrypt_entity(enc, self.aes_key)
                            rows = dec.get("data", []) if isinstance(dec, dict) else []

                            # Add id_keluarga_parent for each row
                            for row in rows:
                                if isinstance(row, dict):
                                    idk = row.get("id_keluarga") or row.get("ID_KELUARGA")
                                    if idk:
                                        row["id_keluarga_parent"] = idk

                            return rows

                        except Exception as e:
                            print(f"[Validation] Decryption failed for entity {index}: {e}")
                            if index == 0:
                                raise DecryptionError(f"Gagal dekripsi: {str(e)}", original_error=e)
                            return []

                    elif response.status_code == 401:
                        if index == 0:
                            raise NetworkError("Token kadaluarsa (401)", status_code=401)
                        return []
                    elif response.status_code == 403:
                        if index == 0:
                            raise NetworkError("Akses ditolak (403)", status_code=403)
                        return []
                    else:
                        if index == 0:
                            raise NetworkError(f"API Error: {response.status_code}", status_code=response.status_code)
                        return []

                except requests.RequestException as e:
                    if index == 0:
                        raise NetworkError(f"Koneksi gagal: {str(e)}", original_error=e)
                    return []

            # Use ThreadPoolExecutor for parallel validation
            max_workers = min(10, len(self.entities))
            completed_count = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_index = {
                    executor.submit(validate_single_entity, entity, i): i
                    for i, entity in enumerate(self.entities)
                }

                for future in as_completed(future_to_index):
                    if self.isInterruptionRequested():
                        executor.shutdown(wait=False)
                        break

                    try:
                        rows = future.result()
                        all_families.extend(rows)
                        completed_count += 1

                        self.status_update.emit(f"🔍 Memeriksa entity {completed_count}/{len(self.entities)}...")

                    except Exception as e:
                        if "Token kadaluarsa" in str(e) or "Akses ditolak" in str(e) or "Gagal dekripsi" in str(e):
                            raise e
                        print(f"[Validation] Worker error: {e}")

            if not self.isInterruptionRequested():
                # Deduplicate families by id_keluarga
                self.status_update.emit("🔄 Menghapus duplikat berdasarkan ID Keluarga...")
                unique_families, duplicates_removed = deduplicate_families(all_families)

                # Store for later use and emit
                self._collected_families = unique_families
                self.families_extracted.emit(unique_families)

                # Generate and display summary
                total_raw = len(all_families)
                summary = get_deduplication_summary(total_raw, duplicates_removed)
                self.status_update.emit(summary)

                # Emit validated signal with unique count
                self.validated.emit(self.authorization, self.entities, len(unique_families))

        except Exception as e:
            self.error.emit(str(e))
            self.status_update.emit("❌ Validasi gagal")
