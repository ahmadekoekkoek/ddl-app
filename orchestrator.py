#!/usr/bin/env python3
"""
orchestrator.py - Simplified workflow with automatic unlock
No WhatsApp OTP - automatic unlock after payment
Background threading for payment polling to prevent GUI freeze
"""

import os
import json
import secrets
import shutil
from typing import Dict
from PySide6.QtCore import QObject, Signal, Slot, QThread
from dotenv import load_dotenv

from scrape_and_build import InMemoryScraper
from file_lock import encrypt_dual_versions, unlock_to_file
from unlock_generator import generate_unlock_code

# Lazy import to avoid circular dependency
WhatsAppOrderManager = None
WhatsAppOrder = None
OrderStatus = None

def _import_payment_components():
    global WhatsAppOrderManager, WhatsAppOrder, OrderStatus
    if WhatsAppOrderManager is None:
        from gui.whatsapp_payment_stage import WhatsAppOrderManager as WOM, WhatsAppOrder as WO, OrderStatus as OS
        WhatsAppOrderManager = WOM
        WhatsAppOrder = WO
        OrderStatus = OS

load_dotenv()


class Orchestrator(QObject):
    """State machine with manual payment flow via WhatsApp"""

    # Signals
    section_progress = Signal(str, int, int)  # section_name, current, total
    stage_changed = Signal(str)
    error_occurred = Signal(str)
    package_selection_requested = Signal(int, int)  # families_count, members_count
    manual_payment_requested = Signal(object)  # Emits Order object
    unlock_code_requested = Signal(object)  # Emits Order object for unlock
    success_completed = Signal(str)
    metrics_updated = Signal(float, int)  # speed_per_sec, eta_seconds

    def __init__(self):
        super().__init__()
        self.config = {}
        self.results = None
        self.tx_id = None
        self.selected_package = None
        self.output_folder = None
        self.locked_folder = None
        self.unlocked_folder = None
        self.api_aes_key = None
        self.bearer_token = None
        self.custom_output_folder = None  # User-selected output folder
        self.scraper_instance = None  # Store scraper instance for metrics
        self.families_count = 0  # Number of Keluarga scraped for pricing
        self.members_count = 0  # Number of members scraped
        self.SECRET_SALT = os.getenv("SECRET_SALT", "DTSEN_PROD_SALT_v2_2024_")

        # Order manager for manual payment (lazy initialized)
        self._order_manager = None
        self.current_order = None

        # Unlock mode attributes (for unlocking existing locked files)
        self.unlock_mode = False
        self.unlock_directory = None

        # Pre-captured families for optimized scraping (skip API fetch)
        self.pre_captured_families: list = []

        # Flexible pricing configuration (Rupiah per Keluarga)
        self.PRICE_PER_KELUARGA = {
            "BASIC": 75,  # Rp 75 per Keluarga
            "PRO": 140    # Rp 140 per Keluarga
        }

        self._check_api_key()

    @property
    def order_manager(self):
        """Lazy-initialize order manager to avoid circular imports."""
        if self._order_manager is None:
            _import_payment_components()
            self._order_manager = WhatsAppOrderManager()
        return self._order_manager

    def _check_api_key(self):
        self.api_aes_key = os.getenv("AES_BASE64_KEY")
        if not self.api_aes_key:
            print("⚠️  WARNING: AES_BASE64_KEY not set!")

    def load_config(self, config_path: str = "config.json") -> Dict:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = json.load(f)
        else:
            self.config = {}
        return self.config

    def save_config(self, token: str, entity_lines: str, output_folder: str = None, config_path: str = "config.json"):
        self.config = {
            "bearer_token": token,
            "entity_lines": entity_lines,
        }
        # Save custom output folder if provided
        if output_folder:
            self.config["output_folder"] = output_folder
            self.custom_output_folder = output_folder

        with open(config_path, "w") as f:
            json.dump(self.config, f, indent=2)

    def _progress_callback(self, section: str, current: int, total: int):
        """Wrapper to emit the section_progress signal and metrics"""
        self.section_progress.emit(section, current, total)

        # Emit metrics if scraper instance is available
        if self.scraper_instance and hasattr(self.scraper_instance, '_calculate_scraping_metrics'):
            try:
                metrics = self.scraper_instance._calculate_scraping_metrics(current, total)
                if metrics['speed'] > 0:  # Only emit if we have valid metrics
                    self.metrics_updated.emit(metrics['speed'], metrics['eta_seconds'])
            except Exception as e:
                pass  # Silently ignore metrics errors

    @Slot()
    def start_process(self):
        self.api_aes_key = os.getenv("AES_BASE64_KEY")
        if not self.api_aes_key:
            self.error_occurred.emit("❌ API AES Key Missing!\n\nSet AES_BASE64_KEY environment variable.")
            return

        if not self.config.get("bearer_token"):
            self.error_occurred.emit("❌ Bearer Token Missing!\n\nPlease save configuration first.")
            return

        self.bearer_token = self.config["bearer_token"]
        print(f"✅ API AES key loaded ({len(self.api_aes_key)})")
        print(f"✅ Bearer token loaded ({len(self.bearer_token)})")

        try:
            self.stage_changed.emit("Scraping data...")
            self._do_scraping()

            self.stage_changed.emit("Mengenkripsi file...")
            self._do_encryption()

            # Calculate counts
            families_count = 0
            members_count = 0
            if self.results and "files" in self.results:
                files = self.results["files"]
                if "families_raw.csv" in files:
                    # Count lines - 1 (header)
                    families_count = max(0, len(files["families_raw.csv"].splitlines()) - 1)
                if "members_raw.csv" in files:
                    members_count = max(0, len(files["members_raw.csv"].splitlines()) - 1)

            # Store counts for pricing calculation
            self.families_count = families_count
            self.members_count = members_count

            self.stage_changed.emit("Pilih paket...")
            self.package_selection_requested.emit(families_count, members_count)

        except Exception as e:
            self.error_occurred.emit(f"Proses gagal: {e}")

    def _do_scraping(self):
        """Execute scraping with per-section progress"""
        entities = self.config["entity_lines"].strip().splitlines()

        self.scraper_instance = InMemoryScraper(
            self.bearer_token,
            self.api_aes_key,
            self._progress_callback
        )

        # Use pre-captured families if available (optimized scraping)
        if self.pre_captured_families:
            print(f"📋 Using {len(self.pre_captured_families)} pre-captured families (skipping API fetch)")
            self.results = self.scraper_instance.run_full_pipeline(
                entities,
                pre_captured_families=self.pre_captured_families
            )
        else:
            self.results = self.scraper_instance.run_full_pipeline(entities)
        self.tx_id = self.results["tx_id"]

        # Wipe credentials
        print("🧹 Wiping credentials from memory...")
        self.api_aes_key = secrets.token_hex(64)
        self.bearer_token = secrets.token_hex(64)
        import gc
        gc.collect()

    def _do_encryption(self):
        """Encrypt all files"""
        # Use custom output folder if set, otherwise use default
        if self.custom_output_folder:
            base_folder = self.custom_output_folder
        elif 'output_folder' in self.config and self.config['output_folder']:
            base_folder = self.config['output_folder']
        else:
            # Default to Desktop/DTSEN_Output
            from system_info import get_default_output_folder
            base_folder = get_default_output_folder()

        self.output_folder = os.path.join(base_folder, self.tx_id)
        self.locked_folder = os.path.join(self.output_folder, "locked")
        self.unlocked_folder = os.path.join(self.output_folder, "unlocked")
        os.makedirs(self.locked_folder, exist_ok=True)
        os.makedirs(self.unlocked_folder, exist_ok=True)

        meta = {
            "tx_id": self.tx_id,
            "timestamp": secrets.token_hex(16),
            "files": list(self.results["files"].keys()),
            "locked_folder": self.locked_folder,
            "unlocked_folder": self.unlocked_folder
        }
        with open(os.path.join(self.output_folder, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)

        total_files = len(self.results["files"])
        for idx, (filename, file_bytes) in enumerate(self.results["files"].items(), 1):
            self.section_progress.emit(f"Encrypting {filename}", idx, total_files)
            encrypt_dual_versions(
                file_bytes,
                filename,
                self.locked_folder,
                self.SECRET_SALT,
                self.tx_id
            )

    @Slot(str)
    def handle_package_selected(self, package: str):
        """User selected a package, create manual payment order"""
        self.selected_package = package
        self.stage_changed.emit("Membuat pesanan...")
        self._create_manual_payment()

    def _create_manual_payment(self):
        """Create manual payment order and notify seller via Twilio"""
        # Calculate flexible pricing based on number of Keluarga
        price_per_keluarga = self.PRICE_PER_KELUARGA.get(self.selected_package, 75)
        amount = int(self.families_count * price_per_keluarga)

        # Minimum amount check (in case of very few families)
        min_amount = 1000  # Minimum Rp 1,000
        amount = max(amount, min_amount)

        print(f"💰 Flexible Pricing: {self.families_count} Keluarga × Rp {price_per_keluarga} = Rp {amount:,}")

        # Generate unlock code for this transaction
        unlock_code = generate_unlock_code(
            self.SECRET_SALT,
            self.selected_package,
            self.tx_id
        )
        print(f"🔑 Generated unlock code: {unlock_code}")

        # Create order
        package_name = f"DTSEN {self.selected_package}"
        self.current_tx_id = self.tx_id  # Store for payment stage
        self.current_order = self.order_manager.create_order(
            tx_id=self.tx_id,
            package_name=package_name,
            amount=amount,
            families_count=self.families_count,
            members_count=self.members_count,
            files_path=self.locked_folder or ""
        )

        # Store unlock code in order
        self.order_manager.set_unlock_code(self.tx_id, unlock_code)

        print(f"📋 Order created: {self.current_order.tx_id}")
        print(f"💰 Amount: Rp {amount:,}")

        # Send Twilio notification to seller
        try:
            from core.twilio_notifier import send_order_notification
            success, result = send_order_notification(
                tx_id=self.tx_id,
                package=self.selected_package,
                family_count=self.families_count,
                amount=amount,
                unlock_code=unlock_code
            )
            if success:
                print(f"📲 Seller notified via Twilio: {result}")
            else:
                print(f"⚠️ Twilio notification failed: {result}")
        except Exception as e:
            print(f"⚠️ Twilio notification error: {e}")

        # Emit signal for GUI to show payment stage
        self.manual_payment_requested.emit(self.current_order)

    @Slot(object)
    def handle_unlock_verified(self, order):
        """Handle when unlock code is verified"""
        self.current_order = order
        print(f"🔓 Unlock verified for order: {order.tx_id}")
        self._handle_payment_success()

    def simulate_payment_success(self, invoice_id=None):
        """Sandbox helper to simulate a successful payment.
        - Guarded by SANDBOX_MODE=1 to prevent misuse in production.
        """
        sandbox_mode = os.getenv("SANDBOX_MODE", "0").strip()

        # Allow skip if SANDBOX_MODE=1
        is_sandbox = sandbox_mode == "1"

        if not is_sandbox:
            self.error_occurred.emit("❌ simulate_payment_success is only available in sandbox mode\n(SANDBOX_MODE=1)")
            return

        print("[SANDBOX] simulate_payment_success invoked")
        if invoice_id:
            print(f"[SANDBOX] invoice_id provided: {invoice_id}")

        # Route through the standard success path
        self._handle_payment_success()

    def _handle_payment_success(self):
        """Payment successful - AUTOMATICALLY UNLOCK FILES"""
        self.stage_changed.emit("Payment confirmed! Unlocking files...")

        print(f"💳 Payment successful for {self.selected_package} package")
        print(f"🔓 Auto-unlocking files for TX: {self.tx_id}")

        # Generate unlock code (for logging/verification)
        unlock_code = generate_unlock_code(
            self.SECRET_SALT,
            self.selected_package,
            self.tx_id
        )
        print(f"🔑 Unlock code (for reference): {unlock_code}")

        # Automatically unlock files
        self._auto_unlock_files()

    def _auto_unlock_files(self):
        """Automatically unlock files after payment"""
        try:
            package = self.selected_package.upper()
            package_lower = package.lower()  # File extensions are lowercase

            # Handle unlock mode (existing locked files from user-selected directory)
            if self.unlock_mode and self.unlock_directory:
                locked_dir = self.unlock_directory
                unlocked_dir = os.path.join(self.unlock_directory, "unlocked")
                self.output_folder = self.unlock_directory  # Set for success message
                print(f"🔓 UNLOCK MODE: Using user-selected directory: {locked_dir}")
            else:
                locked_dir = self.locked_folder or self.output_folder
                unlocked_dir = self.unlocked_folder or os.path.join(self.output_folder, "unlocked")

            os.makedirs(unlocked_dir, exist_ok=True)
            self.unlocked_folder = unlocked_dir  # Store for _organize_unlocked_outputs

            print(f"DEBUG: Looking for files in: {locked_dir}")
            print(f"DEBUG: Package type: {package} (searching for .locked_{package_lower})")

            # List all files - recursively scan in unlock mode
            if self.unlock_mode and self.unlock_directory:
                # Recursively find all locked files
                all_files = []
                locked_files_with_paths = []
                for root, dirs, files in os.walk(locked_dir):
                    for f in files:
                        all_files.append(f)
                        if f.endswith(f".locked_{package_lower}") or f.endswith(".locked"):
                            # Store tuple of (relative_path, full_path, filename)
                            rel_path = os.path.relpath(root, locked_dir)
                            full_path = os.path.join(root, f)
                            locked_files_with_paths.append((rel_path, full_path, f))
                print(f"DEBUG: Total files found (recursive): {len(all_files)}")
                print(f"DEBUG: Locked files found: {len(locked_files_with_paths)}")
            else:
                # Standard non-recursive search
                all_files = os.listdir(locked_dir)
                print(f"DEBUG: All files in folder: {all_files}")
                locked_files_with_paths = []
                for f in all_files:
                    if f.endswith(f".locked_{package_lower}"):
                        locked_files_with_paths.append((".", os.path.join(locked_dir, f), f))

            locked_files = [item[2] for item in locked_files_with_paths]  # Just filenames for logging
            print(f"DEBUG: Locked files found: {locked_files}")

            # FILTER LOGIC: If BASIC, only unlock CSVs
            if package == "BASIC":
                print("ℹ️ BASIC package selected: Filtering for .csv only")
                locked_files_with_paths = [item for item in locked_files_with_paths if ".csv" in item[2]]
                locked_files = [item[2] for item in locked_files_with_paths]
                print(f"DEBUG: Filtered locked files: {locked_files}")

            if not locked_files_with_paths:
                error_msg = f"❌ No locked files found for package '{package}'\n\nFiles in folder: {all_files[:20]}..."
                print(error_msg)
                self.error_occurred.emit(error_msg)
                return

            print(f"🔓 Unlocking {len(locked_files_with_paths)} files...")

            for idx, (rel_path, locked_path, locked_file) in enumerate(locked_files_with_paths, 1):
                # Determine original name by removing the .locked_xxx extension
                if f".locked_{package_lower}" in locked_file:
                    original_name = locked_file.rsplit(f".locked_{package_lower}", 1)[0]
                elif ".locked" in locked_file:
                    original_name = locked_file.rsplit(".locked", 1)[0]
                else:
                    original_name = locked_file

                # Preserve directory structure in unlock mode
                if rel_path and rel_path != ".":
                    output_subdir = os.path.join(unlocked_dir, rel_path)
                    os.makedirs(output_subdir, exist_ok=True)
                    output_path = os.path.join(output_subdir, original_name)
                else:
                    output_path = os.path.join(unlocked_dir, original_name)

                print(f"DEBUG: Unlocking {locked_file} -> {original_name}")

                self.section_progress.emit(f"Unlocking {original_name}", idx, len(locked_files_with_paths))

                # In unlock mode, pass None for tx_id so it reads from the file
                tx_id_to_use = None if self.unlock_mode else self.tx_id
                unlock_to_file(locked_path, output_path, self.SECRET_SALT, tx_id_to_use, package)

                print(f"  ✅ Unlocked: {original_name}")

            print(f"\n✅ Successfully unlocked {len(locked_files_with_paths)} files")
            self._organize_unlocked_outputs()
            print(f"📁 Output root reorganized at: {self.output_folder}")

            # Emit success signal
            self.success_completed.emit(self.output_folder)

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"ERROR in _auto_unlock_files:")
            print(error_details)
            self.error_occurred.emit(f"❌ Failed to unlock files: {e}\n\n{error_details}")

    def _organize_unlocked_outputs(self):
        if not self.unlocked_folder or not os.path.isdir(self.unlocked_folder):
            return
        csv_dir = os.path.join(self.output_folder, "csv")
        pdf_dir = os.path.join(self.output_folder, "pdf")
        os.makedirs(csv_dir, exist_ok=True)
        os.makedirs(pdf_dir, exist_ok=True)

        for file_name in os.listdir(self.unlocked_folder):
            src = os.path.join(self.unlocked_folder, file_name)
            if os.path.isdir(src):
                continue
            ext = os.path.splitext(file_name)[1].lower()
            if ext == ".csv":
                dest_dir = csv_dir
            elif ext == ".pdf":
                dest_dir = pdf_dir
            else:
                dest_dir = self.output_folder
            dest = os.path.join(dest_dir, file_name)
            if os.path.exists(dest):
                os.remove(dest)
            shutil.move(src, dest)

        shutil.rmtree(self.unlocked_folder, ignore_errors=True)
        shutil.rmtree(self.locked_folder, ignore_errors=True)
        self.unlocked_folder = self.output_folder
        self.locked_folder = None
        self._refresh_meta_structure(csv_dir, pdf_dir)

    def _refresh_meta_structure(self, csv_dir: str, pdf_dir: str):
        meta_path = os.path.join(self.output_folder, "meta.json")
        if not os.path.exists(meta_path):
            return
        try:
            with open(meta_path, "r") as f:
                meta = json.load(f)
        except Exception:
            meta = {}
        meta.update({
            "locked_folder": None,
            "unlocked_folder": self.output_folder,
            "structure": {
                "csv": os.path.relpath(csv_dir, self.output_folder),
                "pdf": os.path.relpath(pdf_dir, self.output_folder),
            }
        })
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
