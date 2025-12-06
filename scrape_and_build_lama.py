#!/usr/bin/env python3
"""
scrape_and_build.py - Enhanced with master sheets, desil breakdown, and detailed PDF reports
"""

import os
import io
import json
import time
import base64
import hmac
import hashlib
import secrets
import tempfile
import html
import locale
import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
import pandas as pd
import numpy as np
import warnings
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes

# Use httpx for async HTTP calls (preferred)
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    import requests
from reportlab.lib.pagesizes import A4, landscape, portrait
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.pdfgen import canvas
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

# Optional visualization libraries
try:
    from matplotlib_venn import venn3, venn2
    VENN_AVAILABLE = True
except Exception:
    VENN_AVAILABLE = False

# --- CONSTANTS FOR PDF REPORT ---
PAGE_SIZE_F4 = (21.5 * cm, 33.0 * cm)

FAMILY_HEADERS = [
    ("No KK", ["no_kk", "NO_KK"]),
    ("Nama Kepala Keluarga", ["nama_kepala_keluarga", "NAMA_KEPALA_KELUARGA", "nama_kk"]),
    ("Jumlah Anggota Keluarga", ["jumlah_anggota_calc", "jumlah_anggota", "jml_anggota", "jumlah_art"]),
    ("Alamat", ["alamat", "ALAMAT", "alamat_lengkap"]),
    ("RT", ["no_rt", "rt", "RT"]),
    ("RW", ["no_rw", "rw", "RW"]),
    ("Desil Nasional", ["desil_nasional", "desil", "DESIL"]),
    ("Peringkat Nasional", ["peringkat_nasional", "peringkat"]),
]

MEMBER_HEADERS = [
    ("Nama", ["nama", "nama_lengkap", "NAMA_LENGKAP", "nama_anggota"]),
    ("NIK", ["nik", "NIK", "nik_anggota"]),
    ("Tgl Lahir", ["tgl_lahir", "tanggal_lahir", "TGL_LAHIR"]),
    ("Jenis Kelamin", ["gender_clean", "jenis_kelamin", "jenkel", "id_jenis_kelamin"]),
    ("Hubungan Keluarga", ["hub_kepala_keluarga", "hubungan_keluarga", "hubungan", "status_hubungan"]),
    ("Status Kawin", ["sts_kawin", "status_kawin"]),
]

ASSET_MAPPING = [
    # Immovable Assets (Aset Tidak Bergerak)
    ("Status Penguasaan Bangunan", ["status_penguasaan_bangunan", "status_lahan", "kepemilikan_rumah"]),
    ("Lantai Terluas", ["jenis_lantai", "lantai_terluas", "lantai"]),
    ("Dinding Terluas", ["jenis_dinding", "dinding_terluas", "dinding"]),
    ("Atap Terluas", ["jenis_atap", "atap_terluas", "atap"]),
    ("Sumber Air Minum", ["sumber_air_minum", "air_minum"]),
    ("Jarak Sumber Air Limbah", ["jarak_sumber_air_limbah", "jarak_tinja", "jarak_pembuangan"]),
    ("Sumber Penerangan", ["sumber_penerangan", "penerangan"]),
    ("Bahan Bakar Utama", ["bahan_bakar_utama", "bahan_bakar_memasak", "bahan_bakar"]),
    ("Fasilitas BAB", ["fasilitas_bab", "kepemilikan_kamar_mandi"]),
    ("Jenis Kloset", ["jenis_kloset", "kloset"]),
    ("Pembuangan Tinja", ["pembuangan_tinja", "tempat_pembuangan_akhir_tinja"]),
    # Movable Assets - Livestock (Aset Bergerak - Ternak)
    ("Jumlah Sapi", ["jml_sapi", "jumlah_sapi", "sapi"]),
    ("Jumlah Kerbau", ["jml_kerbau", "jumlah_kerbau", "kerbau"]),
    ("Jumlah Kuda", ["jml_kuda", "jumlah_kuda", "kuda"]),
    ("Jumlah Babi", ["jml_babi", "jumlah_babi", "babi"]),
    ("Jumlah Kambing/Domba", ["jml_kambing_domba", "jumlah_kambing", "jumlah_domba", "kambing", "domba"]),
    # Land Assets
    ("Luas Sawah/Kebun", ["luas_sawah", "luas_kebun", "luas_lahan"]),
    ("Lahan Tempat Lain", ["lahan_tempat_lain", "lahan_lain"]),
    ("Rumah Tempat Lain", ["rumah_tempat_lain", "rumah_lain"]),
    # Movable Assets - Electronics & Appliances (Aset Bergerak)
    ("Air Conditioner (AC)", ["ac", "air_conditioner"]),
    ("Emas/Perhiasan min. 10 gr", ["emas", "perhiasan"]),
    ("Kapal/Perahu Motor", ["kapal_perahu_motor", "kapal", "perahu_motor"]),
    ("Komputer/Laptop/Tablet", ["komputer", "laptop", "tablet"]),
    ("Lemari Es/Kulkas", ["lemari_es", "kulkas"]),
    ("Mobil", ["mobil"]),
    ("Pemanas Air (Water Heater)", ["pemanas_air", "water_heater"]),
    ("Perahu", ["perahu"]),
    ("Sepeda", ["sepeda"]),
    ("Sepeda Motor", ["sepeda_motor", "motor"]),
    ("Smartphone", ["smartphone", "hp"]),
    ("Tabung Gas 5.5 kg atau lebih", ["tabung_gas"]),
    ("Telepon Rumah (PSTN)", ["telepon_rumah", "telepon"]),
    ("Televisi Layar Datar min 30 inch", ["televisi", "tv_flat", "tv"]),
]

# API Endpoints
URL_FAMILY = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-keluarga-dtsen"
URL_MEMBERS = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-anggota-keluarga-dtsen-by-id-keluarga"
URL_KYC = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-daftar-kyc-keluarga-dtsen"
URL_PBI = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-pbi-by-id-keluarga"
URL_PKH = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-by-id-keluarga"
URL_BPNT = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-bpnt-by-id-keluarga"
URL_ASET = "https://api.kemensos.go.id/dtsen/aset/v1/get-aset-keluarga-by-id-keluarga"
URL_ASET_BERGERAK = "https://api.kemensos.go.id/dtsen/aset/v1/get-aset-keluarga-bergerak-by-id-keluarga"

# Scraper config - OPTIMIZED FOR PERFORMANCE
THREADS_PER_PROCESS = 20  # Concurrency for per-family endpoints (PKH/BPNT/PBI)
SLEEP_BETWEEN_REQUESTS = 0.15  # Slightly lower to improve throughput while staying polite
RETRY_LIMIT = 3
TIMEOUT = 40  # Increased from 25 to 40 to reduce timeout errors on slow connections


class UnauthorizedError(RuntimeError):
    """Raised when API responds with HTTP 401."""
    pass

def safe_b64decode(s: str) -> bytes:
    s = (s or "").strip().replace("\n", "").replace(" ", "")
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s)

def decrypt_entity(entity_b64: str, key_b64: str) -> Any:
    outer = json.loads(safe_b64decode(entity_b64).decode("utf-8"))
    iv = safe_b64decode(outer["iv"])
    ciphertext = safe_b64decode(outer["value"])
    mac_expected = outer.get("mac")
    key = safe_b64decode(key_b64)
    mac_data = base64.b64encode(iv).decode() + base64.b64encode(ciphertext).decode()
    mac_calc = hmac.new(key, mac_data.encode(), hashlib.sha256).hexdigest()
    if mac_expected and mac_calc != mac_expected:
        raise ValueError("MAC mismatch")
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
    try:
        return json.loads(plaintext.decode("utf-8"))
    except:
        return plaintext.decode("utf-8")

def encrypt_entity_payload(payload_obj: Any, key_b64: str) -> str:
    key = safe_b64decode(key_b64)
    iv = get_random_bytes(16)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))
    mac_data = base64.b64encode(iv).decode() + base64.b64encode(ciphertext).decode()
    mac = hmac.new(key, mac_data.encode(), hashlib.sha256).hexdigest()
    outer = {"iv": base64.b64encode(iv).decode(), "value": base64.b64encode(ciphertext).decode(), "mac": mac}
    return base64.b64encode(json.dumps(outer, separators=(",",":")).encode()).decode()

def safe_post(url: str, data: Dict, headers: Dict):
    """Sync HTTP POST with retry logic (uses httpx if available, else requests).
    Note: This is kept for compatibility in isolated calls, but InMemoryScraper._post should be preferred.
    """
    backoff = 1.0
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            if HTTPX_AVAILABLE:
                with httpx.Client(timeout=TIMEOUT) as client:
                    r = client.post(url, data=data, headers=headers)
            else:
                import requests as _req
                r = _req.post(url, data=data, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code == 401:
                print("[HTTP] 401 Unauthorized")
                raise UnauthorizedError("HTTP 401 Unauthorized")
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"[HTTP] 429 Too Many Requests - sleeping {delay:.2f}s")
                time.sleep(delay)
                continue
            print(f"[HTTP] {r.status_code}: {r.text[:200]}")
        except UnauthorizedError:
            raise
        except Exception as e:
            print(f"[HTTP] Error: {e}")
        time.sleep(backoff)
        backoff *= 1.8
    return None


async def async_post(url: str, data: Dict, headers: Dict, client: "httpx.AsyncClient") -> Optional["httpx.Response"]:
    """Async HTTP POST with retry logic using httpx"""
    if not HTTPX_AVAILABLE:
        raise ImportError("httpx not installed. Run: pip install httpx")

    backoff = 1.0
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            r = await client.post(url, data=data, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code == 401:
                print(f"[HTTP] 401 Unauthorized")
                raise UnauthorizedError("HTTP 401 Unauthorized")
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"[HTTP] 429 Too Many Requests - sleeping {delay:.2f}s")
                await asyncio.sleep(delay)
                continue
            print(f"[HTTP] {r.status_code}: {r.text[:200]}")
        except UnauthorizedError:
            raise
        except Exception as e:
            print(f"[HTTP] Error: {e}")
        await asyncio.sleep(backoff)
        backoff *= 1.8
    return None

# Helper functions from merge.py
def compute_age_from_str(s):
    """Calculate age from date of birth string"""
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    if s in ("", "-", "nan", "none"):
        return np.nan
    for fmt in ("%Y-%m-%d","%d-%m-%Y","%d/%m/%Y","%Y/%m/%d","%Y%m%d"):
        try:
            bd = datetime.strptime(s, fmt).date()
            today = date.today()
            return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
        except:
            continue
    try:
        y = int(s[:4])
        return date.today().year - y
    except:
        return np.nan

def map_desil(v):
    """Map desil values to standard labels"""
    try:
        if pd.isna(v) or str(v).strip() in ("", "0", "nan", "None", "-"):
            return "DESIL_BELUM_DITENTUKAN"
        s = str(v).strip()
        if s.isdigit():
            n = int(s)
            if 1 <= n <= 5:
                return f"DESIL_{n}"
            if 6 <= n <= 10:
                return "DESIL_6_10"
        return "DESIL_BELUM_DITENTUKAN"
    except:
        return "DESIL_BELUM_DITENTUKAN"

def make_bansos_combo(has_pkh, has_bpnt, has_pbi):
    """Generate bansos combination string"""
    parts = []
    if has_pkh: parts.append("PKH")
    if has_bpnt: parts.append("BPNT")
    if has_pbi: parts.append("PBI")
    return "_".join(parts) if parts else "NO_BANSOS"

def autofit_columns_xlsx(worksheet, df, start_col=0):
    """Auto-fit column widths in Excel"""
    for i, col in enumerate(df.columns):
        try:
            max_len = max(df[col].astype(str).map(len).max() if not df.empty else 0, len(str(col))) + 2
        except:
            max_len = len(str(col)) + 2
        max_len = min(max_len, 60)
        worksheet.set_column(start_col + i, start_col + i, max_len)

class InMemoryScraper:
    def __init__(self, bearer_token: str, aes_key_b64: str, progress_cb: Callable[[str, int, int], None] = None):
        self.token = bearer_token  # NO "Bearer " prefix
        self.aes_key = aes_key_b64
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Authorization": bearer_token,
            "Origin": "https://siks.kemensos.go.id",
            "Referer": "https://siks.kemensos.go.id/",
        }
        self.progress_cb = progress_cb

        # Scraping metrics tracking
        self.scrape_start_time = None
        self.total_entities = 0
        self.completed_families = 0

        # Concurrent progress tracking
        self.concurrent_total = 0
        self.concurrent_current = 0

        # Persistent HTTP client/session
        self.client = None
        try:
            if HTTPX_AVAILABLE:
                limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
                self.client = httpx.Client(headers=self.headers, timeout=TIMEOUT, http2=True, limits=limits)
            else:
                import requests as _req
                self.client = _req.Session()
                self.client.headers.update(self.headers)
        except Exception:
            self.client = None

        # Per-endpoint tuning
        # workers: max concurrency for per-keluarga endpoints
        # sleep_range: polite inter-request delay with jitter (min, max) in seconds
        self.endpoint_tuning = {
            "PKH": {"workers": 16, "sleep_range": (0.05, 0.15)},
            "BPNT": {"workers": 16, "sleep_range": (0.05, 0.15)},
            "PBI": {"workers": 16, "sleep_range": (0.06, 0.18)},
            "DEFAULT": {"workers": THREADS_PER_PROCESS, "sleep_range": (SLEEP_BETWEEN_REQUESTS, SLEEP_BETWEEN_REQUESTS + 0.1)},
        }

    def _update_progress(self, section: str, current: int, total: int):
        """Safely call progress callback if provided"""
        if self.progress_cb and callable(self.progress_cb):
            try:
                self.progress_cb(section, current, total)
            except Exception as e:
                print(f"Progress callback error: {e}")

    def _calculate_scraping_metrics(self, current: int, total: int) -> dict:
        """Calculate real-time scraping speed and ETA"""
        if self.scrape_start_time is None:
            return {'speed': 0.0, 'eta_seconds': 0}

        import time
        elapsed = time.time() - self.scrape_start_time

        if elapsed < 1 or current == 0:
            return {'speed': 0.0, 'eta_seconds': 0}

        # Calculate speed (Keluarga/second)
        speed = current / elapsed

        # Calculate ETA
        remaining = total - current
        eta_seconds = int(remaining / speed) if speed > 0 else 0

        return {
            'speed': round(speed, 2),
            'eta_seconds': eta_seconds
        }

    def _jitter_sleep(self, label: str):
        """Sleep with small jitter based on endpoint label."""
        import random, time as _t
        tuning = self.endpoint_tuning.get(label.upper(), self.endpoint_tuning["DEFAULT"])
        lo, hi = tuning.get("sleep_range", (SLEEP_BETWEEN_REQUESTS, SLEEP_BETWEEN_REQUESTS + 0.1))
        delay = random.uniform(lo, hi)
        _t.sleep(delay)

    def _post(self, url: str, data: Dict) -> Optional[Any]:
        """POST using persistent client with 401/429 aware retries."""
        backoff = 1.0
        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                if self.client is None:
                    # Fallback
                    r = safe_post(url, data, self.headers)
                    return r
                if HTTPX_AVAILABLE and isinstance(self.client, httpx.Client):
                    r = self.client.post(url, data=data)
                else:
                    r = self.client.post(url, data=data, timeout=TIMEOUT)
                status = getattr(r, 'status_code', None)
                if status == 200:
                    return r
                if status == 401:
                    print("[HTTP] 401 Unauthorized")
                    raise UnauthorizedError("HTTP 401 Unauthorized")
                if status == 429:
                    retry_after = getattr(r, 'headers', {}).get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after and str(retry_after).isdigit() else backoff
                    except Exception:
                        delay = backoff
                    print(f"[HTTP] 429 Too Many Requests - sleeping {delay:.2f}s")
                    time.sleep(delay)
                    backoff *= 1.5
                    continue
                # Other errors
                body_preview = r.text[:200] if hasattr(r, 'text') else ''
                print(f"[HTTP] {status}: {body_preview}")
            except UnauthorizedError:
                raise
            except Exception as e:
                print(f"[HTTP] Error: {e}")
            time.sleep(backoff)
            backoff *= 1.8
        return None

    def _close_client(self):
        try:
            if self.client is None:
                return
            if HTTPX_AVAILABLE and isinstance(self.client, httpx.Client):
                self.client.close()
            else:
                close = getattr(self.client, 'close', None)
                if callable(close):
                    close()
        except Exception as e:
            print(f"[HTTP] Client close error: {e}")

    def fetch_families(self, entity_lines: List[str]) -> List[Dict]:
        import time as time_module
        self.scrape_start_time = time_module.time()
        self.total_entities = len(entity_lines)
        self.completed_families = 0

        self._update_progress("[INFO] Fetching families", 0, len(entity_lines))
        families = []
        for idx, entity in enumerate(entity_lines, 1):
            self.completed_families = idx
            self._update_progress("[INFO] Fetching families", idx, len(entity_lines))
            r = self._post(URL_FAMILY, {"entity": entity})
            if not r:
                continue
            try:
                resp_json = r.json()
                enc = resp_json.get("data") if isinstance(resp_json, dict) else r.text.strip('"')
                if not enc:
                    continue
                dec = decrypt_entity(enc, self.aes_key)
                rows = dec.get("data", []) if isinstance(dec, dict) else []
                for row in rows:
                    idk = row.get("id_keluarga") or row.get("ID_KELUARGA")
                    row["id_keluarga_parent"] = idk
                families.extend(rows)
            except Exception as e:
                print(f"[Families] Error: {e}")
            self._jitter_sleep("FAMILIES")
        return families

    def fetch_endpoint(self, label: str, url: str, id_list: List[str]) -> List[Dict]:
        self._update_progress(f"[FETCH] Fetching {label}", 0, len(id_list))
        results = []
        def fetch_one(id_keluarga: str) -> List[Dict]:
            try:
                payload = {"id_keluarga": id_keluarga}
                entity = encrypt_entity_payload(payload, self.aes_key)
            except Exception as e:
                print(f"[{label}] Encrypt failed: {e}")
                return []
            r = self._post(url, {"entity": entity})
            if not r:
                return []
            try:
                resp_json = r.json()
                if isinstance(resp_json, dict) and resp_json.get("status") and "data" in resp_json:
                    enc = resp_json["data"]
                else:
                    enc = r.text.strip().strip('"')
            except:
                enc = r.text.strip().strip('"')
            if not enc:
                return []
            try:
                decrypted = decrypt_entity(enc, self.aes_key)
            except Exception as e:
                print(f"[{label}] Decrypt failed: {e}")
                return []
            extracted = []
            if isinstance(decrypted, dict) and "data" in decrypted:
                extracted = decrypted["data"] or []
            elif isinstance(decrypted, list):
                extracted = decrypted
            elif isinstance(decrypted, dict):
                extracted = [decrypted]
            if isinstance(extracted, list) and len(extracted) == 1 and isinstance(extracted[0], list):
                extracted = extracted[0]
            for item in extracted:
                if isinstance(item, dict):
                    item["id_keluarga_parent"] = id_keluarga
            self._jitter_sleep(label)
            return extracted

        # Tune workers per endpoint label
        tuning = self.endpoint_tuning.get(label.upper(), self.endpoint_tuning["DEFAULT"])
        max_workers = tuning.get("workers", THREADS_PER_PROCESS)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fetch_one, idk): idk for idk in id_list}
            for idx, future in enumerate(as_completed(futures), 1):
                self._update_progress(f"[FETCH] Fetching {label}", idx, len(id_list))
                try:
                    rows = future.result()
                    results.extend(rows)
                except Exception as e:
                    if isinstance(e, UnauthorizedError):
                        raise
                    print(f"[{label}] Worker error: {e}")
        return results

    def clean_aset(self, aset_rows: List[Dict]) -> pd.DataFrame:
        """Fixed: always create minimal DataFrame"""
        if not aset_rows:
            return pd.DataFrame(columns=["id_keluarga_parent"])

        df = pd.json_normalize(aset_rows)
        if "id_keluarga" not in df.columns and "id_keluarga_parent" in df.columns:
            df["id_keluarga"] = df["id_keluarga_parent"]

        drop_cols = [c for c in df.columns if "/" in c and c.split("/", 1)[0].isdigit()]
        if drop_cols:
            df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

        if df.empty:
            return pd.DataFrame(columns=["id_keluarga_parent"])

        return df

    def clean_aset_bergerak(self, asetb_rows: List[Dict]) -> pd.DataFrame:
        """Fixed: ensure DataFrame is never truly empty"""
        if not asetb_rows:
            return pd.DataFrame(columns=["id_keluarga"])

        alias_map = {
            # Livestock
            "sapi": "jumlah_sapi",
            "jumlahsapi": "jumlah_sapi",
            "kerbau": "jumlah_kerbau",
            "jumlahkerbau": "jumlah_kerbau",
            "kambing": "jumlah_kambing",
            "domba": "jumlah_kambing",
            "kambingdomba": "jumlah_kambing",
            "jumlahkambingdomba": "jumlah_kambing",
            "babi": "jumlah_babi",
            "jumlahbabi": "jumlah_babi",
            "kuda": "jumlah_kuda",
            "jumlahkuda": "jumlah_kuda",
            # Electronics & Appliances
            "ac": "ac",
            "airconditioner": "ac",
            "acairconditioner": "ac",
            "airconditionerac": "ac",
            "emas": "emas",
            "perhiasan": "emas",
            "emasperhiasan": "emas",
            "emasperhiasanmin10gram": "emas",
            "komputer": "komputer",
            "laptop": "komputer",
            "tablet": "komputer",
            "komputerlaptoptablet": "komputer",
            "lemaries": "lemari_es",
            "lemarieskulkas": "lemari_es",
            "kulkas": "lemari_es",
            "mobil": "mobil",
            "sepeda": "sepeda",
            "sepedamotor": "sepeda_motor",
            "motor": "sepeda_motor",
            "smartphone": "smartphone",
            "hp": "smartphone",
            "televisi": "televisi",
            "tv": "televisi",
            "tvflat": "televisi",
            "televisilayardatar": "televisi",
            "televisilayardatarmin30inci": "televisi",
            # NEW: Missing assets
            "kapal": "kapal_perahu_motor",
            "perahumotor": "kapal_perahu_motor",
            "kapalperahumotor": "kapal_perahu_motor",
            "pemanasair": "pemanas_air",
            "waterheater": "pemanas_air",
            "pemanasairwaterheater": "pemanas_air",
            "perahu": "perahu",
            "tabunggas": "tabung_gas",
            "tabunggas55kg": "tabung_gas",
            "tabunggas55kgataulebih": "tabung_gas",
            "telepon": "telepon_rumah",
            "teleponrumah": "telepon_rumah",
            "teleponrumahpstn": "telepon_rumah",
            "pstn": "telepon_rumah",
        }

        def normalize_label(value: Any) -> str:
            text = str(value or "").lower()
            return "".join(ch for ch in text if ch.isalnum())

        totals: Dict[str, Dict[str, int]] = {}

        def register_entry(id_keluarga: Any, jenis: Any, jumlah_val: Any):
            if not id_keluarga:
                return
            norm = normalize_label(jenis)
            canonical = alias_map.get(norm)
            if not canonical:
                return
            try:
                jumlah = int(pd.to_numeric(jumlah_val, errors="coerce") or 0)
            except Exception:
                jumlah = 0
            if jumlah <= 0:
                return
            fam_id = str(id_keluarga)
            per_family = totals.setdefault(fam_id, {})
            per_family[canonical] = per_family.get(canonical, 0) + jumlah

        for rec in asetb_rows:
            if not isinstance(rec, dict):
                continue
            family_id = rec.get("id_keluarga") or rec.get("id_keluarga_parent")
            if "jenis_aset" in rec and ("jumlah" in rec or "jml" in rec):
                register_entry(family_id, rec.get("jenis_aset"), rec.get("jumlah", rec.get("jml")))
            for key in ("data", "aset", "aset_bergerak", "items", "rows"):
                if key in rec and isinstance(rec[key], list):
                    for item in rec[key]:
                        if isinstance(item, dict):
                            register_entry(
                                family_id,
                                item.get("jenis_aset") or item.get("jenis"),
                                item.get("jumlah", item.get("jml"))
                            )

        if not totals:
            return pd.DataFrame(columns=["id_keluarga"])

        rows = []
        for fam_id, data in totals.items():
            row = {"id_keluarga": fam_id}
            row.update(data)
            rows.append(row)
        return pd.DataFrame(rows)

    async def fetch_families_async(self, entity_lines: List[str]) -> List[Dict]:
        """Async version of fetch_families using httpx"""
        if not HTTPX_AVAILABLE:
            print("[WARN] httpx not available, falling back to sync fetch")
            return self.fetch_families(entity_lines)

        import time as time_module
        self.scrape_start_time = time_module.time()
        self.total_entities = len(entity_lines)
        self.completed_families = 0

        self._update_progress("[INFO] Fetching families (async)", 0, len(entity_lines))
        families = []

        async with httpx.AsyncClient(headers=self.headers, timeout=TIMEOUT) as client:
            for idx, entity in enumerate(entity_lines, 1):
                self.completed_families = idx
                self._update_progress("[INFO] Fetching families (async)", idx, len(entity_lines))

                r = await async_post(URL_FAMILY, {"entity": entity}, self.headers, client)
                if not r:
                    continue
                try:
                    resp_json = r.json()
                    enc = resp_json.get("data") if isinstance(resp_json, dict) else r.text.strip('"')
                    if not enc:
                        continue
                    dec = decrypt_entity(enc, self.aes_key)
                    rows = dec.get("data", []) if isinstance(dec, dict) else []
                    for row in rows:
                        idk = row.get("id_keluarga") or row.get("ID_KELUARGA")
                        row["id_keluarga_parent"] = idk
                    families.extend(rows)
                except Exception as e:
                    print(f"[Families Async] Error: {e}")
                await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
        return families

    async def fetch_endpoint_async(self, label: str, url: str, id_list: List[str], use_concurrent_progress=False) -> List[Dict]:
        """Async version with bounded concurrency and jitter per endpoint."""
        if not HTTPX_AVAILABLE:
            print("[WARN] httpx not available, falling back to sync fetch")
            return self.fetch_endpoint(label, url, id_list)

        # Tuning per label
        tuning = self.endpoint_tuning.get(label.upper(), self.endpoint_tuning["DEFAULT"])
        max_workers = tuning.get("workers", 8)

        if not use_concurrent_progress:
            self._update_progress(f"[FETCH] Fetching {label} (async)", 0, len(id_list))

        results: List[Dict] = []
        sem = asyncio.Semaphore(max_workers)

        async def _async_jitter_sleep(lbl: str):
            import random
            lo, hi = tuning.get("sleep_range", (SLEEP_BETWEEN_REQUESTS, SLEEP_BETWEEN_REQUESTS + 0.1))
            await asyncio.sleep(random.uniform(lo, hi))

        async def worker(id_keluarga: str) -> List[Dict]:
            async with sem:
                try:
                    payload = {"id_keluarga": id_keluarga}
                    entity = encrypt_entity_payload(payload, self.aes_key)
                except Exception as e:
                    print(f"[{label}] Encrypt failed: {e}")
                    return []

                # Create shared AsyncClient with HTTP/2 and limits
                return_rows: List[Dict] = []
                return_rows_local: List[Dict] = []
                try:
                    async with httpx.AsyncClient(headers=self.headers, timeout=TIMEOUT, http2=True,
                                                 limits=httpx.Limits(max_connections=50, max_keepalive_connections=20)) as client:
                        r = await async_post(url, {"entity": entity}, self.headers, client)
                        if not r:
                            return []
                        try:
                            resp_json = r.json()
                            if isinstance(resp_json, dict) and resp_json.get("status") and "data" in resp_json:
                                enc = resp_json["data"]
                            else:
                                enc = r.text.strip().strip('"')
                        except:
                            enc = r.text.strip().strip('"')
                        if not enc:
                            return []
                        try:
                            decrypted = decrypt_entity(enc, self.aes_key)
                        except Exception as e:
                            print(f"[{label}] Decrypt failed: {e}")
                            return []
                        if isinstance(decrypted, dict) and "data" in decrypted:
                            return_rows_local = decrypted["data"] or []
                        elif isinstance(decrypted, list):
                            return_rows_local = decrypted
                        elif isinstance(decrypted, dict):
                            return_rows_local = [decrypted]
                        if isinstance(return_rows_local, list) and len(return_rows_local) == 1 and isinstance(return_rows_local[0], list):
                            return_rows_local = return_rows_local[0]
                        for item in return_rows_local:
                            if isinstance(item, dict):
                                item["id_keluarga_parent"] = id_keluarga
                        return_rows.extend(return_rows_local)
                finally:
                    await _async_jitter_sleep(label)
                    if use_concurrent_progress:
                        self.concurrent_current += 1
                        self._update_progress(f"[FETCH] Scraping all data...", self.concurrent_current, self.concurrent_total)
                return return_rows

        tasks = [asyncio.create_task(worker(i)) for i in id_list]
        idx = 0
        for coro in asyncio.as_completed(tasks):
            rows = await coro
            results.extend(rows)
            idx += 1
            if not use_concurrent_progress:
                self._update_progress(f"[FETCH] Fetching {label} (async)", idx, len(id_list))

        return results

    def build_keluarga_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Build comprehensive family-level master sheet"""
        print("[DATA] Building KELUARGA_MASTER...")

        # Load data
        families = pd.read_csv(io.BytesIO(files_dict.get("families_raw.csv", b""))) if "families_raw.csv" in files_dict else pd.DataFrame()
        pkh = pd.read_csv(io.BytesIO(files_dict.get("pkh_raw.csv", b""))) if "pkh_raw.csv" in files_dict else pd.DataFrame()
        bpnt = pd.read_csv(io.BytesIO(files_dict.get("bpnt_raw.csv", b""))) if "bpnt_raw.csv" in files_dict else pd.DataFrame()
        pbi = pd.read_csv(io.BytesIO(files_dict.get("pbi_raw.csv", b""))) if "pbi_raw.csv" in files_dict else pd.DataFrame()
        aset = pd.read_csv(io.BytesIO(files_dict.get("aset_merged.csv", b""))) if "aset_merged.csv" in files_dict else pd.DataFrame()

        if families.empty:
            return pd.DataFrame()

        # Add bansos flags
        families["has_pkh"] = families["id_keluarga"].isin(pkh.get("id_keluarga", pd.Series(dtype=str)))
        families["has_bpnt"] = families["id_keluarga"].isin(bpnt.get("id_keluarga", pd.Series(dtype=str)))

        pbi_family_ids = set()
        if "id_keluarga_parent" in pbi.columns:
            pbi_family_ids.update(pbi["id_keluarga_parent"].astype(str).fillna("").tolist())
        if "id_keluarga" in pbi.columns:
            pbi_family_ids.update(pbi["id_keluarga"].astype(str).fillna("").tolist())
        families["has_pbi"] = families["id_keluarga"].astype(str).isin(pbi_family_ids)

        # Bansos combo
        families["bansos_combo"] = families.apply(
            lambda r: make_bansos_combo(r.get("has_pkh"), r.get("has_bpnt"), r.get("has_pbi")),
            axis=1
        )

        # Desil classification
        if "desil_nasional" in families.columns:
            families["desil_class"] = families["desil_nasional"].apply(map_desil)
        elif "desil" in families.columns:
            families["desil_class"] = families["desil"].apply(map_desil)
        else:
            families["desil_class"] = "DESIL_BELUM_DITENTUKAN"

        # Merge assets
        if not aset.empty and "id_keluarga" in aset.columns:
            families = families.merge(aset, on="id_keluarga", how="left", suffixes=("", "_aset"))

        return families

    def build_anggota_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Build comprehensive member-level master sheet"""
        print("[DATA] Building ANGGOTA_MASTER...")

        # Load data
        members = pd.read_csv(io.BytesIO(files_dict.get("members_raw.csv", b""))) if "members_raw.csv" in files_dict else pd.DataFrame()
        kyc = pd.read_csv(io.BytesIO(files_dict.get("kyc_raw.csv", b""))) if "kyc_raw.csv" in files_dict else pd.DataFrame()
        pbi = pd.read_csv(io.BytesIO(files_dict.get("pbi_raw.csv", b""))) if "pbi_raw.csv" in files_dict else pd.DataFrame()
        families = pd.read_csv(io.BytesIO(files_dict.get("families_raw.csv", b""))) if "families_raw.csv" in files_dict else pd.DataFrame()

        if members.empty:
            return pd.DataFrame()

        members_full = members.copy()

        # Merge KYC
        if not kyc.empty and "idsemesta" in members_full.columns and "idsemesta" in kyc.columns:
            members_full = members_full.merge(kyc, on="idsemesta", how="left", suffixes=("", "_kyc"))

        # Merge PBI (via NIK)
        pbi_nik_col = next((c for c in ("nik", "NIK", "nik_input") if c in pbi.columns), None)
        member_nik_col = next((c for c in ("nik", "NIK") if c in members_full.columns), None)
        if pbi_nik_col and member_nik_col and not pbi.empty:
            members_full = members_full.merge(pbi, left_on=member_nik_col, right_on=pbi_nik_col, how="left", suffixes=("", "_pbi"))

        # Bring family context (bansos_combo, desil_class)
        if not families.empty and "id_keluarga" in members_full.columns and "id_keluarga" in families.columns:
            # Build bansos flags for families first
            pkh = pd.read_csv(io.BytesIO(files_dict.get("pkh_raw.csv", b""))) if "pkh_raw.csv" in files_dict else pd.DataFrame()
            bpnt = pd.read_csv(io.BytesIO(files_dict.get("bpnt_raw.csv", b""))) if "bpnt_raw.csv" in files_dict else pd.DataFrame()

            families_temp = families.copy()
            families_temp["has_pkh"] = families_temp["id_keluarga"].isin(pkh.get("id_keluarga", pd.Series(dtype=str)))
            families_temp["has_bpnt"] = families_temp["id_keluarga"].isin(bpnt.get("id_keluarga", pd.Series(dtype=str)))

            pbi_family_ids = set()
            if "id_keluarga_parent" in pbi.columns:
                pbi_family_ids.update(pbi["id_keluarga_parent"].astype(str).fillna("").tolist())
            if "id_keluarga" in pbi.columns:
                pbi_family_ids.update(pbi["id_keluarga"].astype(str).fillna("").tolist())
            families_temp["has_pbi"] = families_temp["id_keluarga"].astype(str).isin(pbi_family_ids)

            families_temp["bansos_combo"] = families_temp.apply(
                lambda r: make_bansos_combo(r.get("has_pkh"), r.get("has_bpnt"), r.get("has_pbi")),
                axis=1
            )

            if "desil_nasional" in families_temp.columns:
                families_temp["desil_class"] = families_temp["desil_nasional"].apply(map_desil)
            elif "desil" in families_temp.columns:
                families_temp["desil_class"] = families_temp["desil"].apply(map_desil)
            else:
                families_temp["desil_class"] = "DESIL_BELUM_DITENTUKAN"

            members_full = members_full.merge(
                families_temp[["id_keluarga", "bansos_combo", "desil_class"]],
                on="id_keluarga",
                how="left"
            )

        # Calculate age
        dob_candidates = ["tgl_lahir", "tgl_lahir_anggota", "tgl_lahir_input", "tgl_lahir_kyc"]
        birth_col = next((c for c in dob_candidates if c in members_full.columns), None)
        if birth_col:
            members_full["age"] = members_full[birth_col].apply(compute_age_from_str)
        else:
            members_full["age"] = pd.NA

        # Clean gender
        gender_candidates = ["jenkel", "id_jenis_kelamin", "jenis_kelamin", "gender"]
        gender_col = next((c for c in gender_candidates if c in members_full.columns), None)
        members_full["gender_clean"] = members_full.get(gender_col, "").fillna("").replace({"": "Unknown"})

        return members_full

    def build_desil_sheets(self, keluarga_master: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Split keluarga_master by desil_class"""
        print("[DATA] Building DESIL sheets...")

        desil_labels = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
        desil_sheets = {}

        for label in desil_labels:
            filtered = keluarga_master[keluarga_master["desil_class"] == label] if not keluarga_master.empty else pd.DataFrame()
            desil_sheets[label] = filtered

        return desil_sheets

    def create_visualizations(
        self,
        files_dict: Dict[str, bytes],
        keluarga_master: Optional[pd.DataFrame] = None,
        anggota_master: Optional[pd.DataFrame] = None
    ) -> Dict[str, str]:
        """Create visualization charts"""
        print("[DATA] Creating visualizations...")

        tmpdir = tempfile.mkdtemp(prefix="charts_")
        charts: Dict[str, str] = {}

        try:
            families_raw = pd.read_csv(io.BytesIO(files_dict.get("families_raw.csv", b""))) if "families_raw.csv" in files_dict else pd.DataFrame()
            members_raw = pd.read_csv(io.BytesIO(files_dict.get("members_raw.csv", b""))) if "members_raw.csv" in files_dict else pd.DataFrame()
            pkh = pd.read_csv(io.BytesIO(files_dict.get("pkh_raw.csv", b""))) if "pkh_raw.csv" in files_dict else pd.DataFrame()
            bpnt = pd.read_csv(io.BytesIO(files_dict.get("bpnt_raw.csv", b""))) if "bpnt_raw.csv" in files_dict else pd.DataFrame()
            pbi = pd.read_csv(io.BytesIO(files_dict.get("pbi_raw.csv", b""))) if "pbi_raw.csv" in files_dict else pd.DataFrame()

            keluarga_df = keluarga_master.copy() if isinstance(keluarga_master, pd.DataFrame) else self.build_keluarga_master(files_dict)
            if keluarga_df is None:
                keluarga_df = pd.DataFrame()
            members_df = anggota_master.copy() if isinstance(anggota_master, pd.DataFrame) else self.build_anggota_master(files_dict)
            if members_df is None or members_df.empty:
                members_df = members_raw.copy()
            else:
                members_df = members_df.copy()

            families_context = keluarga_df.copy() if not keluarga_df.empty else families_raw.copy()
            if not families_context.empty and "desil_class" not in families_context.columns:
                if "desil_nasional" in families_context.columns:
                    families_context["desil_class"] = families_context["desil_nasional"].apply(map_desil)
                elif "desil" in families_context.columns:
                    families_context["desil_class"] = families_context["desil"].apply(map_desil)
                else:
                    families_context["desil_class"] = "DESIL_BELUM_DITENTUKAN"

            if not members_df.empty:
                if "age" not in members_df.columns:
                    dob_candidates = ["tgl_lahir", "tgl_lahir_anggota", "tgl_lahir_input", "tgl_lahir_kyc"]
                    birth_col = next((c for c in dob_candidates if c in members_df.columns), None)
                    if birth_col:
                        members_df["age"] = members_df[birth_col].apply(compute_age_from_str)
                    else:
                        members_df["age"] = pd.NA
                if "gender_clean" not in members_df.columns:
                    gender_candidates = ["jenkel", "id_jenis_kelamin", "jenis_kelamin", "gender"]
                    gender_col = next((c for c in gender_candidates if c in members_df.columns), None)
                    if gender_col:
                        members_df["gender_clean"] = members_df[gender_col].fillna("").replace({"": "Unknown"})
                    else:
                        members_df["gender_clean"] = "Unknown"
                if "desil_class" not in members_df.columns and "id_keluarga" in members_df.columns and not families_context.empty and "id_keluarga" in families_context.columns:
                    members_df = members_df.merge(
                        families_context[["id_keluarga", "desil_class"]],
                        on="id_keluarga",
                        how="left"
                    )

            def fallback_bar(labels: List[str], counts: List[int], title: str, path: str, color: str = "#2F75B5"):
                plt.figure(figsize=(max(6, len(labels) * 0.8), 4.5))
                positions = list(range(len(labels)))
                plt.bar(positions, counts, color=color)
                plt.xticks(positions, labels, rotation=30, ha="right")
                plt.title(title)
                plt.tight_layout()
                plt.savefig(path, dpi=150)
                plt.close()

            # BANSOS Venn Diagram
            try:
                bansos_png = os.path.join(tmpdir, "bansos_venn.png")
                set_pkh = set(pkh.get("id_keluarga", pd.Series(dtype=str)).astype(str).fillna("").tolist())
                set_bpnt = set(bpnt.get("id_keluarga", pd.Series(dtype=str)).astype(str).fillna("").tolist())
                set_pbi = set()
                if "id_keluarga_parent" in pbi.columns:
                    set_pbi.update(pbi["id_keluarga_parent"].astype(str).fillna("").tolist())
                if "id_keluarga" in pbi.columns:
                    set_pbi.update(pbi["id_keluarga"].astype(str).fillna("").tolist())

                if VENN_AVAILABLE:
                    plt.figure(figsize=(6, 6))
                    venn3([set_pkh, set_bpnt, set_pbi], ("PKH", "BPNT", "PBI"))
                    plt.title("BANSOS Overlap (PKH / BPNT / PBI)")
                    plt.tight_layout()
                    plt.savefig(bansos_png, dpi=150)
                    plt.close()
                else:
                    only_pkh = len(set_pkh - set_bpnt - set_pbi)
                    only_bpnt = len(set_bpnt - set_pkh - set_pbi)
                    only_pbi = len(set_pbi - set_pkh - set_bpnt)
                    inter_12 = len((set_pkh & set_bpnt) - set_pbi)
                    inter_13 = len((set_pkh & set_pbi) - set_bpnt)
                    inter_23 = len((set_bpnt & set_pbi) - set_pkh)
                    inter_123 = len(set_pkh & set_bpnt & set_pbi)
                    labels = ["PKH only", "BPNT only", "PBI only", "PKH+BPNT", "PKH+PBI", "BPNT+PBI", "All three"]
                    counts = [only_pkh, only_bpnt, only_pbi, inter_12, inter_13, inter_23, inter_123]
                    fallback_bar(labels, counts, "Irisan BANSOS", bansos_png)

                charts["bansos_venn"] = bansos_png
            except Exception as e:
                print(f"BANSOS chart failed: {e}")

            # DESIL Venn
            try:
                desil_candidates = [
                    "desil_nasional",
                    "desil_provinsi",
                    "desil_kabupaten",
                    "desil_kecamatan",
                    "desil_desa"
                ]
                available_cols = [c for c in desil_candidates if c in families_raw.columns]
                if len(available_cols) < 2:
                    extra_cols = [c for c in families_raw.columns if "desil" in c.lower() and c not in available_cols]
                    available_cols.extend(extra_cols)
                desil_cols = available_cols[:3]

                def normalize_desil(value: Any) -> Optional[str]:
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        return None
                    s = str(value).strip().upper()
                    if not s or s in {"-", "NAN", "NONE"}:
                        return None
                    s = s.replace("-", "_")
                    tokens = [tok for tok in s.split("_") if tok]
                    for tok in tokens:
                        if tok.isdigit():
                            return f"DESIL_{tok}"
                    if s.startswith("DESIL"):
                        return s
                    return None

                low_values = {f"DESIL_{i}" for i in range(1, 6)}
                if len(desil_cols) >= 2 and not families_raw.empty:
                    desil_sets: List[set] = []
                    labels: List[str] = []
                    for col in desil_cols:
                        col_set = set()
                        for _, row in families_raw.iterrows():
                            fid = row.get("id_keluarga") or row.get("ID_KELUARGA") or row.get("id_keluarga_parent")
                            if fid is None or (isinstance(fid, float) and pd.isna(fid)):
                                continue
                            norm = normalize_desil(row.get(col))
                            if norm and norm in low_values:
                                col_set.add(str(fid))
                        desil_sets.append(col_set)
                        labels.append(col.replace("_", " ").upper())

                    if any(len(s) for s in desil_sets):
                        desil_png = os.path.join(tmpdir, "desil_venn.png")
                        try:
                            if VENN_AVAILABLE and len(desil_sets) == 3:
                                plt.figure(figsize=(6, 6))
                                venn3(desil_sets, tuple(labels[:3]))
                                plt.title("Irisan DESIL (Pendapatan Rendah)")
                                plt.tight_layout()
                                plt.savefig(desil_png, dpi=150)
                                plt.close()
                            elif VENN_AVAILABLE and len(desil_sets) == 2:
                                plt.figure(figsize=(6, 6))
                                venn2(desil_sets[:2], tuple(labels[:2]))
                                plt.title("Irisan DESIL (Pendapatan Rendah)")
                                plt.tight_layout()
                                plt.savefig(desil_png, dpi=150)
                                plt.close()
                            else:
                                counts = [len(s) for s in desil_sets]
                                fallback_bar(labels, counts, "Jumlah Prioritas DESIL", desil_png, "#27ae60")
                        except Exception as inner_err:
                            counts = [len(s) for s in desil_sets]
                            fallback_bar(labels, counts, "Jumlah Prioritas DESIL", desil_png, "#27ae60")
                        charts["desil_venn"] = desil_png
            except Exception as e:
                print(f"DESIL venn failed: {e}")

            # Age-based visualizations
            try:
                if not members_df.empty:
                    ages = pd.to_numeric(members_df.get("age", pd.Series(dtype=float)), errors="coerce").dropna()
                    if not ages.empty:
                        age_png = os.path.join(tmpdir, "age_hist.png")
                        plt.figure(figsize=(8, 4))
                        bins = list(range(0, 101, 5))
                        plt.hist(ages, bins=bins, edgecolor="black")
                        plt.title("Distribusi Usia (Anggota)")
                        plt.xlabel("Usia")
                        plt.ylabel("Jumlah")
                        plt.tight_layout()
                        plt.savefig(age_png, dpi=150)
                        plt.close()
                        charts["age_hist"] = age_png

                    if "desil_class" in members_df.columns:
                        desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
                        age_desil = members_df.dropna(subset=["age", "desil_class"]).copy()
                        age_desil["age"] = pd.to_numeric(age_desil["age"], errors="coerce")
                        age_desil = age_desil.dropna(subset=["age"])
                        if not age_desil.empty:
                            medians = age_desil.groupby("desil_class")["age"].median().reindex(desil_order)
                            medians = medians.dropna()
                            if not medians.empty:
                                age_desil_png = os.path.join(tmpdir, "age_by_desil.png")
                                fallback_bar(medians.index.tolist(), medians.values.tolist(), "Median Usia per DESIL", age_desil_png, "#8e44ad")
                                charts["age_by_desil"] = age_desil_png
            except Exception as e:
                print(f"Age chart failed: {e}")

            # Gender pie chart
            try:
                if not members_df.empty and "gender_clean" in members_df.columns:
                    gender_counts = members_df["gender_clean"].fillna("Unknown").value_counts()
                    if not gender_counts.empty:
                        gender_png = os.path.join(tmpdir, "gender_pie.png")
                        plt.figure(figsize=(5, 5))
                        plt.pie(gender_counts, labels=gender_counts.index.tolist(), autopct="%1.1f%%", startangle=90)
                        plt.title("Distribusi Jenis Kelamin (Anggota)")
                        plt.tight_layout()
                        plt.savefig(gender_png, dpi=150)
                        plt.close()
                        charts["gender_pie"] = gender_png
            except Exception as e:
                print(f"Gender chart failed: {e}")

            # DESIL distribution bar
            try:
                if not families_context.empty and "desil_class" in families_context.columns:
                    desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
                    desil_counts = families_context["desil_class"].value_counts().reindex(desil_order, fill_value=0)
                    if desil_counts.sum() > 0:
                        desil_dist_png = os.path.join(tmpdir, "desil_distribution.png")
                        fallback_bar(desil_counts.index.tolist(), desil_counts.values.tolist(), "Keluarga per DESIL", desil_dist_png, "#16a085")
                        charts["desil_distribution"] = desil_dist_png
            except Exception as e:
                print(f"DESIL distribution failed: {e}")

            # === NEW VISUALIZATIONS FROM a.txt ===

            # 1. Population Pyramid (Butterfly Chart)
            try:
                if not members_df.empty and "age" in members_df.columns and "gender_clean" in members_df.columns:
                    age_col = pd.to_numeric(members_df["age"], errors="coerce")
                    valid_members = members_df[age_col.notna()].copy()
                    valid_members["age"] = age_col[age_col.notna()]
                    valid_members["age_group"] = pd.cut(valid_members["age"], bins=range(0, 101, 5), right=False)

                    if not valid_members.empty:
                        gender_age = valid_members.groupby(["age_group", "gender_clean"]).size().unstack(fill_value=0)

                        male_col = next((c for c in gender_age.columns if "laki" in str(c).lower() or "male" in str(c).lower() or c == "1"), None)
                        female_col = next((c for c in gender_age.columns if "perempuan" in str(c).lower() or "female" in str(c).lower() or c == "2"), None)

                        if male_col is not None and female_col is not None:
                            pyramid_png = os.path.join(tmpdir, "population_pyramid.png")

                            fig, ax = plt.subplots(figsize=(10, 8))
                            y_pos = range(len(gender_age.index))

                            ax.barh(y_pos, -gender_age[male_col], color='#3498db', label='Laki-laki')
                            ax.barh(y_pos, gender_age[female_col], color='#e74c3c', label='Perempuan')

                            ax.set_yticks(y_pos)
                            ax.set_yticklabels([str(interval).replace("(", "").replace("]", "").replace(",", "-") for interval in gender_age.index])
                            ax.set_xlabel('Populasi')
                            ax.set_title('Piramida Penduduk (Usia & Gender)')
                            ax.legend()
                            ax.axvline(0, color='black', linewidth=0.8)
                            ax.set_xticks(ax.get_xticks())
                            ax.set_xticklabels([abs(int(x)) for x in ax.get_xticks()])

                            plt.tight_layout()
                            plt.savefig(pyramid_png, dpi=150)
                            plt.close()
                            charts["population_pyramid"] = pyramid_png
            except Exception as e:
                print(f"Population pyramid failed: {e}")

            # 2. Household Size Distribution
            try:
                if not families_context.empty:
                    size_col = None
                    for col in ["jumlah_anggota_calc", "jumlah_anggota", "jml_anggota", "jumlah_art"]:
                        if col in families_context.columns:
                            size_col = col
                            break

                    if size_col:
                        sizes = pd.to_numeric(families_context[size_col], errors="coerce").dropna()
                        if not sizes.empty:
                            size_bins = [0, 1, 2, 3, 4, 5, 10, 999]
                            size_labels = ["1", "2", "3", "4", "5", "6-10", "10+"]
                            size_groups = pd.cut(sizes, bins=size_bins, labels=size_labels, right=False)
                            size_counts = size_groups.value_counts().sort_index()

                            household_png = os.path.join(tmpdir, "household_size.png")
                            plt.figure(figsize=(8, 5))
                            plt.bar(range(len(size_counts)), size_counts.values, color='#16a085')
                            plt.xticks(range(len(size_counts)), size_counts.index)
                            plt.xlabel('Jumlah Anggota Keluarga')
                            plt.ylabel('Jumlah Keluarga')
                            plt.title('Distribusi Ukuran Rumah Tangga')
                            plt.tight_layout()
                            plt.savefig(household_png, dpi=150)
                            plt.close()
                            charts["household_size"] = household_png
            except Exception as e:
                print(f"Household size distribution failed: {e}")

            # 3. Head of Household Profile (Gender)
            try:
                if not members_df.empty:
                    head_col = None
                    for col in ["hub_kepala_keluarga", "hubungan_keluarga", "hubungan", "status_hubungan"]:
                        if col in members_df.columns:
                            head_col = col
                            break

                    if head_col and "gender_clean" in members_df.columns:
                        heads = members_df[members_df[head_col].astype(str).str.contains("kepala", case=False, na=False)]
                        if not heads.empty:
                            gender_heads = heads["gender_clean"].value_counts()

                            hoh_png = os.path.join(tmpdir, "head_of_household.png")
                            fig, ax = plt.subplots(figsize=(6, 6))
                            colors_hoh = ['#3498db', '#e74c3c', '#95a5a6']
                            wedges, texts, autotexts = ax.pie(gender_heads.values, labels=gender_heads.index, autopct='%1.1f%%',
                                                               startangle=90, colors=colors_hoh[:len(gender_heads)])
                            ax.set_title('Profil Kepala Rumah Tangga (Gender)')

                            donut_circle = plt.Circle((0, 0), 0.70, fc='white')
                            fig.gca().add_artist(donut_circle)

                            plt.tight_layout()
                            plt.savefig(hoh_png, dpi=150)
                            plt.close()
                            charts["head_of_household"] = hoh_png
            except Exception as e:
                print(f"Head of household chart failed: {e}")

            # 4. Bansos Penetration by Desil (Stacked Bar 100%)
            try:
                if not families_context.empty and "desil_class" in families_context.columns:
                    bansos_col = "bansos_combo" if "bansos_combo" in families_context.columns else None
                    if bansos_col:
                        desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10"]

                        families_context["has_bansos"] = families_context[bansos_col].apply(
                            lambda x: "Terima Bansos" if x != "NO_BANSOS" else "Tidak Ada"
                        )

                        crosstab = pd.crosstab(families_context["desil_class"], families_context["has_bansos"], normalize='index') * 100
                        crosstab = crosstab.reindex(desil_order, fill_value=0)

                        if not crosstab.empty:
                            penetration_png = os.path.join(tmpdir, "bansos_penetration.png")

                            fig, ax = plt.subplots(figsize=(10, 6))
                            crosstab.plot(kind='barh', stacked=True, ax=ax, color=['#27ae60', '#e74c3c'])
                            ax.set_xlabel('Persentase (%)')
                            ax.set_ylabel('Kelas Desil')
                            ax.set_title('Penetrasi Bansos per Desil (Analisis Inklusi/Eksklusi)')
                            ax.legend(title='Status')
                            ax.set_xlim(0, 100)

                            plt.tight_layout()
                            plt.savefig(penetration_png, dpi=150)
                            plt.close()
                            charts["bansos_penetration"] = penetration_png
            except Exception as e:
                print(f"Bansos penetration chart failed: {e}")

            # 5. Housing Quality Index (Grouped Bar Chart)
            try:
                aset_data = pd.read_csv(io.BytesIO(files_dict.get("aset_merged.csv", b""))) if "aset_merged.csv" in files_dict else pd.DataFrame()

                if not aset_data.empty:
                    housing_indicators = {
                        'Floor': ['jenis_lantai', 'lantai_terluas', 'lantai'],
                        'Wall': ['jenis_dinding', 'dinding_terluas', 'dinding'],
                        'Roof': ['jenis_atap', 'atap_terluas', 'atap']
                    }

                    housing_data = {}
                    for name, cols in housing_indicators.items():
                        col = next((c for c in cols if c in aset_data.columns), None)
                        if col:
                            housing_data[name] = aset_data[col].value_counts().head(5)

                    if housing_data:
                        housing_png = os.path.join(tmpdir, "housing_quality.png")

                        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
                        colors = ['#3498db', '#e74c3c', '#f39c12']

                        for idx, (name, data) in enumerate(housing_data.items()):
                            axes[idx].barh(range(len(data)), data.values, color=colors[idx])
                            axes[idx].set_yticks(range(len(data)))
                            axes[idx].set_yticklabels(data.index, fontsize=8)
                            axes[idx].set_xlabel('Jumlah')
                            axes[idx].set_title(f'{name} Tipe')
                            axes[idx].invert_yaxis()

                        plt.suptitle('Indeks Kualitas Perumahan', fontsize=14, fontweight='bold')
                        plt.tight_layout()
                        plt.savefig(housing_png, dpi=150)
                        plt.close()
                        charts["housing_quality"] = housing_png
            except Exception as e:
                print(f"Housing quality chart failed: {e}")

            # 6. Modern Asset Ownership Heatmap
            try:
                if not aset_data.empty and not families_context.empty:
                    if "id_keluarga" in aset_data.columns and "id_keluarga" in families_context.columns and "desil_class" in families_context.columns:
                        merged_assets = aset_data.merge(
                            families_context[["id_keluarga", "desil_class"]],
                            on="id_keluarga",
                            how="left"
                        )

                        modern_assets = ["mobil", "motor", "sepeda_motor", "kulkas", "lemari_es", "smartphone", "hp",
                                       "televisi", "tv", "komputer", "laptop", "ac"]

                        available_assets = [col for col in modern_assets if col in merged_assets.columns]

                        if available_assets and "desil_class" in merged_assets.columns:
                            desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10"]

                            heatmap_data = []
                            for desil in desil_order:
                                desil_subset = merged_assets[merged_assets["desil_class"] == desil]
                                if not desil_subset.empty:
                                    row = []
                                    for asset in available_assets[:6]:
                                        ownership = (pd.to_numeric(desil_subset[asset], errors='coerce') > 0).sum()
                                        pct = (ownership / len(desil_subset)) * 100 if len(desil_subset) > 0 else 0
                                        row.append(pct)
                                    heatmap_data.append(row)
                                else:
                                    heatmap_data.append([0] * min(6, len(available_assets)))

                            if heatmap_data and any(any(row) for row in heatmap_data):
                                heatmap_png = os.path.join(tmpdir, "asset_ownership_heatmap.png")

                                fig, ax = plt.subplots(figsize=(10, 6))
                                # Removed local import numpy as np
                                im = ax.imshow(heatmap_data, cmap='YlOrRd', aspect='auto')

                                ax.set_xticks(range(min(6, len(available_assets))))
                                ax.set_xticklabels([available_assets[i].replace("_", " ").title() for i in range(min(6, len(available_assets)))], rotation=45, ha='right')
                                ax.set_yticks(range(len(desil_order)))
                                ax.set_yticklabels(desil_order)
                                ax.set_title('Peta Panas Kepemilikan Aset Modern (% per Desil)')

                                cbar = plt.colorbar(im, ax=ax)
                                cbar.set_label('Kepemilikan %', rotation=270, labelpad=15)

                                for i in range(len(desil_order)):
                                    for j in range(min(6, len(available_assets))):
                                        text = ax.text(j, i, f'{heatmap_data[i][j]:.1f}%', ha="center", va="center", color="black", fontsize=8)

                                plt.tight_layout()
                                plt.savefig(heatmap_png, dpi=150)
                                plt.close()
                                charts["asset_ownership_heatmap"] = heatmap_png
            except Exception as e:
                print(f"Asset ownership heatmap failed: {e}")

            # 7. Sanitation & Water Access
            try:
                if not aset_data.empty:
                    water_col = next((c for c in ["sumber_air_minum", "air_minum"] if c in aset_data.columns), None)
                    sanitation_col = next((c for c in ["fasilitas_bab", "kepemilikan_kamar_mandi", "pembuangan_tinja"] if c in aset_data.columns), None)

                    if water_col or sanitation_col:
                        sanitation_png = os.path.join(tmpdir, "sanitation_water.png")

                        fig, axes = plt.subplots(1, 2 if (water_col and sanitation_col) else 1, figsize=(12, 5))
                        if not isinstance(axes, np.ndarray):
                            axes = [axes]

                        plot_idx = 0
                        if water_col:
                            water_data = aset_data[water_col].value_counts().head(5)
                            axes[plot_idx].barh(range(len(water_data)), water_data.values, color='#3498db')
                            axes[plot_idx].set_yticks(range(len(water_data)))
                            axes[plot_idx].set_yticklabels(water_data.index, fontsize=9)
                            axes[plot_idx].set_xlabel('Jumlah')
                            axes[plot_idx].set_title('Sumber Air')
                            axes[plot_idx].invert_yaxis()
                            plot_idx += 1

                        if sanitation_col:
                            sanit_data = aset_data[sanitation_col].value_counts().head(5)
                            axes[plot_idx].barh(range(len(sanit_data)), sanit_data.values, color='#e74c3c')
                            axes[plot_idx].set_yticks(range(len(sanit_data)))
                            axes[plot_idx].set_yticklabels(sanit_data.index, fontsize=9)
                            axes[plot_idx].set_xlabel('Jumlah')
                            axes[plot_idx].set_title('Fasilitas Sanitasi')
                            axes[plot_idx].invert_yaxis()

                        plt.suptitle('Akses Sanitasi & Air', fontsize=14, fontweight='bold')
                        plt.tight_layout()
                        plt.savefig(sanitation_png, dpi=150)
                        plt.close()
                        charts["sanitation_water"] = sanitation_png
            except Exception as e:
                print(f"Sanitation & water chart failed: {e}")

            # 8. Poverty Hotspots by RT/RW
            try:
                if not families_context.empty and "desil_class" in families_context.columns:
                    # Try to find RT/RW columns
                    rt_col = next((c for c in ["rt", "no_rt", "nomor_rt"] if c in families_context.columns), None)
                    rw_col = next((c for c in ["rw", "no_rw", "nomor_rw"] if c in families_context.columns), None)

                    if rt_col and rw_col:
                        # Group by RW (broader) or RT if RW not available
                        hotspots = families_context.groupby([rw_col, rt_col]).size().reset_index(name='count')
                        hotspots = hotspots.sort_values('count', ascending=False).head(10)

                        if not hotspots.empty:
                            hotspots['label'] = "RW " + hotspots[rw_col].astype(str) + " / RT " + hotspots[rt_col].astype(str)

                            hotspot_png = os.path.join(tmpdir, "poverty_hotspots.png")

                            plt.figure(figsize=(10, 6))
                            plt.barh(range(len(hotspots)), hotspots['count'], color='#e74c3c')
                            plt.yticks(range(len(hotspots)), hotspots['label'])
                            plt.xlabel('Jumlah Keluarga')
                            plt.title('Titik Panas Kemiskinan per RT/RW (Top 10)')
                            plt.gca().invert_yaxis()

                            plt.tight_layout()
                            plt.savefig(hotspot_png, dpi=150)
                            plt.close()
                            charts["poverty_hotspots"] = hotspot_png
            except Exception as e:
                print(f"Poverty hotspots chart failed: {e}")


        except Exception as e:
            print(f"Visualization creation error: {e}")

        return charts

    def build_xlsx(self, files_dict: Dict[str, bytes]) -> bytes:
        """Build enhanced XLSX with master sheets, desil breakdown, and visualizations"""
        output = io.BytesIO()

        # Build master sheets
        keluarga_master = self.build_keluarga_master(files_dict)
        anggota_master = self.build_anggota_master(files_dict)
        desil_sheets = self.build_desil_sheets(keluarga_master)

        # Create visualizations
        charts = self.create_visualizations(files_dict, keluarga_master, anggota_master)

        # Load other data
        pkh = pd.read_csv(io.BytesIO(files_dict.get("pkh_raw.csv", b""))) if "pkh_raw.csv" in files_dict else pd.DataFrame()
        bpnt = pd.read_csv(io.BytesIO(files_dict.get("bpnt_raw.csv", b""))) if "bpnt_raw.csv" in files_dict else pd.DataFrame()
        pbi = pd.read_csv(io.BytesIO(files_dict.get("pbi_raw.csv", b""))) if "pbi_raw.csv" in files_dict else pd.DataFrame()
        aset = pd.read_csv(io.BytesIO(files_dict.get("aset_merged.csv", b""))) if "aset_merged.csv" in files_dict else pd.DataFrame()

        # Create summaries
        if not keluarga_master.empty and "bansos_combo" in keluarga_master.columns:
            summary_bansos = keluarga_master.groupby("bansos_combo").size().reset_index(name="jumlah_keluarga").sort_values("jumlah_keluarga", ascending=False)
        else:
            summary_bansos = pd.DataFrame(columns=["bansos_combo", "jumlah_keluarga"])

        if not keluarga_master.empty and "desil_class" in keluarga_master.columns:
            summary_desil = keluarga_master.groupby("desil_class").size().reset_index(name="jumlah_keluarga").sort_values("desil_class")
        else:
            summary_desil = pd.DataFrame(columns=["desil_class", "jumlah_keluarga"])

        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            workbook = writer.book

            # Format definitions
            header_fmt = workbook.add_format({
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#2F75B5",
                "align": "center"
            })
            odd_fmt = workbook.add_format({"bg_color": "#F7F7F7"})
            even_fmt = workbook.add_format({"bg_color": "#FFFFFF"})
            text_fmt = workbook.add_format({'num_format': '@'})
            date_fmt = workbook.add_format({'num_format': 'dd/mm/yyyy'})

            # Columns config - Extended drop list per user request
            cols_to_drop = {
                'id_keluarga', 'id_keluarga_parent', 'idsemesta', 'ID_KELUARGA', 'IDSEMESTA',
                'id_keluarga_aset', 'id_keluarga_parent_pbi',
                # Additional columns to drop per user request
                'id_keluarga_kyc', 'no_kk_kyc', 'nik_input', 'id_jenis_kelamin',
                'id_status_perkawinan', 'id_hub_keluarga', 'id_keluarga_pbi',
                'id_deleted', 'alasan_tolak_meninggal', 'nama_input',
                'no_prop', 'no_kab', 'no_kec', 'no_kel',
                'id_pekerjaan_utama', 'id_keluarga_parent_kyc', 'idsemesta_pbi'
            }
            text_cols = {'nik', 'no_kk', 'NIK', 'NO_KK', 'nik_anggota', 'nomor_kartu', 'nomor_kks', 'nomor_pkh'}
            date_cols = {'tgl_lahir', 'tanggal_lahir', 'TGL_LAHIR', 'tanggal_pencairan', 'tanggal_pembayaran', 'tanggal'}

            def prepare_df_for_excel(df_in):
                if df_in.empty: return df_in
                d = df_in.copy()
                # Drop ID columns
                drop_these = [c for c in d.columns if c in cols_to_drop]
                d.drop(columns=drop_these, inplace=True, errors='ignore')
                # Convert dates
                for c in d.columns:
                    if c in date_cols or 'tanggal' in c.lower() or 'tgl' in c.lower():
                        d[c] = pd.to_datetime(d[c], errors='coerce').dt.date
                return d

            # Write master sheets
            if not keluarga_master.empty:
                # Explicitly ensure 'no_kk' is treated as string to preserve leading zeros
                df_km = prepare_df_for_excel(keluarga_master)
                for col in ["no_kk", "NO_KK", "nik", "NIK"]:
                    if col in df_km.columns:
                        df_km[col] = df_km[col].astype(str)
                df_km.to_excel(writer, sheet_name="KELUARGA_MASTER", index=False)

            if not anggota_master.empty:
                df_am = prepare_df_for_excel(anggota_master)
                for col in ["nik", "NIK", "no_kk", "NO_KK"]:
                    if col in df_am.columns:
                        df_am[col] = df_am[col].astype(str)
                df_am.to_excel(writer, sheet_name="ANGGOTA_MASTER", index=False)

            # Write desil sheets
            for sheet_name, df in desil_sheets.items():
                df_d = prepare_df_for_excel(df)
                for col in ["no_kk", "NO_KK", "nik", "NIK"]:
                    if col in df_d.columns:
                        df_d[col] = df_d[col].astype(str)
                df_d.to_excel(writer, sheet_name=sheet_name, index=False)

            # Write detail sheets
            if not pkh.empty:
                df_pkh = prepare_df_for_excel(pkh)
                for col in ["nik", "NIK", "nomor_pkh", "NOMOR_PKH"]:
                    if col in df_pkh.columns:
                        df_pkh[col] = df_pkh[col].astype(str)
                df_pkh.to_excel(writer, sheet_name="PKH_DETAIL", index=False)

            if not bpnt.empty:
                df_bpnt = prepare_df_for_excel(bpnt)
                for col in ["nik", "NIK", "nomor_kks", "NOMOR_KKS"]:
                    if col in df_bpnt.columns:
                        df_bpnt[col] = df_bpnt[col].astype(str)
                df_bpnt.to_excel(writer, sheet_name="BPNT_DETAIL", index=False)

            if not pbi.empty:
                df_pbi = prepare_df_for_excel(pbi)
                for col in ["nik", "NIK", "nomor_kartu", "NOMOR_KARTU"]:
                    if col in df_pbi.columns:
                        df_pbi[col] = df_pbi[col].astype(str)
                df_pbi.to_excel(writer, sheet_name="PBI_DETAIL", index=False)

            if not aset.empty:
                prepare_df_for_excel(aset).to_excel(writer, sheet_name="ASET_DETAIL", index=False)

            # --- NEW: REKAP_PERINGKAT Sheet ---
            if not keluarga_master.empty:
                print("[XLSX] Building REKAP_PERINGKAT...")
                rekap_df = keluarga_master.copy()

                # Ensure ID is string for merging
                if "id_keluarga" in rekap_df.columns:
                    rekap_df["id_keluarga"] = rekap_df["id_keluarga"].astype(str)

                # Calculate PKH Total Nominal
                pkh_totals = pd.DataFrame(columns=["id_keluarga", "total_pkh"])
                if not pkh.empty and "id_keluarga" in pkh.columns:
                    pkh_temp = pkh.copy()
                    pkh_temp["id_keluarga"] = pkh_temp["id_keluarga"].astype(str)
                    # Clean nominal column
                    def clean_nominal(val):
                        if pd.isna(val): return 0
                        s = str(val).replace("Rp", "").replace(".", "").replace(",", "").strip()
                        try: return int(s)
                        except: return 0

                    nom_col = next((c for c in ["nominal", "jumlah_bantuan", "nominal_bansos"] if c in pkh_temp.columns), None)
                    if nom_col:
                        pkh_temp["nominal_clean"] = pkh_temp[nom_col].apply(clean_nominal)
                        pkh_totals = pkh_temp.groupby("id_keluarga")["nominal_clean"].sum().reset_index(name="total_pkh")

                # Calculate BPNT Total Nominal
                bpnt_totals = pd.DataFrame(columns=["id_keluarga", "total_bpnt"])
                if not bpnt.empty and "id_keluarga" in bpnt.columns:
                    bpnt_temp = bpnt.copy()
                    bpnt_temp["id_keluarga"] = bpnt_temp["id_keluarga"].astype(str)

                    nom_col = next((c for c in ["nominal", "jumlah_bantuan", "nominal_bansos"] if c in bpnt_temp.columns), None)
                    if nom_col:
                        # BPNT nominal cleaning (same logic)
                        def clean_nominal(val):
                            if pd.isna(val): return 0
                            s = str(val).replace("Rp", "").replace(".", "").replace(",", "").strip()
                            try: return int(s)
                            except: return 0
                        bpnt_temp["nominal_clean"] = bpnt_temp[nom_col].apply(clean_nominal)
                        bpnt_totals = bpnt_temp.groupby("id_keluarga")["nominal_clean"].sum().reset_index(name="total_bpnt")

                # Merge totals
                rekap_df = rekap_df.merge(pkh_totals, on="id_keluarga", how="left")
                rekap_df = rekap_df.merge(bpnt_totals, on="id_keluarga", how="left")

                # Fill NaN with 0
                rekap_df["total_pkh"] = rekap_df["total_pkh"].fillna(0)
                rekap_df["total_bpnt"] = rekap_df["total_bpnt"].fillna(0)

                # Calculate Grand Total
                rekap_df["grand_total"] = rekap_df["total_pkh"] + rekap_df["total_bpnt"]

                # Ranking Logic
                # Convert peringkat_nasional to numeric, put errors (non-numeric) at the end
                if "peringkat_nasional" in rekap_df.columns:
                    rekap_df["rank_sort"] = pd.to_numeric(rekap_df["peringkat_nasional"], errors='coerce')
                    # Sort: numeric values first (ascending), then NaNs (which were non-numeric or missing)
                    rekap_df = rekap_df.sort_values("rank_sort", ascending=True, na_position='last')
                    rekap_df.drop(columns=["rank_sort"], inplace=True)

                # Select columns
                cols_to_keep = [
                    "no_kk", "nama_kepala_keluarga", "alamat", "no_rt", "no_rw",
                    "desil_nasional", "peringkat_nasional", "bansos_combo",
                    "total_pkh", "total_bpnt", "grand_total"
                ]
                # Filter existing columns only
                final_cols = [c for c in cols_to_keep if c in rekap_df.columns]
                rekap_final = rekap_df[final_cols].copy()

                # Force no_kk to string to avoid scientific notation
                if "no_kk" in rekap_final.columns:
                    rekap_final["no_kk"] = rekap_final["no_kk"].astype(str)

                # Write to Excel
                rekap_final.to_excel(writer, sheet_name="REKAP_PERINGKAT", index=False)

                # Apply formatting for REKAP_PERINGKAT
                ws_rekap = writer.sheets["REKAP_PERINGKAT"]
                ws_rekap.freeze_panes(1, 0)  # Freeze top row

                currency_fmt = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
                text_fmt_rekap = workbook.add_format({'num_format': '@'}) # Text format

                # Get column indices
                pkh_idx = final_cols.index("total_pkh") if "total_pkh" in final_cols else -1
                bpnt_idx = final_cols.index("total_bpnt") if "total_bpnt" in final_cols else -1
                grand_idx = final_cols.index("grand_total") if "grand_total" in final_cols else -1
                nokk_idx = final_cols.index("no_kk") if "no_kk" in final_cols else -1

                # Apply formats
                if pkh_idx >= 0: ws_rekap.set_column(pkh_idx, pkh_idx, 15, currency_fmt)
                if bpnt_idx >= 0: ws_rekap.set_column(bpnt_idx, bpnt_idx, 15, currency_fmt)
                if grand_idx >= 0: ws_rekap.set_column(grand_idx, grand_idx, 15, currency_fmt)
                if nokk_idx >= 0: ws_rekap.set_column(nokk_idx, nokk_idx, 20, text_fmt_rekap)

                # Autofit columns
                for i, col in enumerate(final_cols):
                    # Skip columns we already set explicit width/format for, OR just override width but keep format?
                    # Better to just ensure width is enough.
                    # Calculate max width
                    max_len = len(str(col)) # Header length
                    column_data = rekap_final[col].astype(str)
                    if not column_data.empty:
                        max_data_len = column_data.map(len).max()
                        if pd.notna(max_data_len):
                            max_len = max(max_len, max_data_len)

                    # Add some padding
                    width = max_len + 2

                    # If we already set a format, we need to preserve it.
                    # set_column(first_col, last_col, width, cell_format, options)
                    # We need to know which format to use.
                    current_fmt = None
                    if i == pkh_idx or i == bpnt_idx or i == grand_idx:
                        current_fmt = currency_fmt
                    elif i == nokk_idx:
                        current_fmt = text_fmt_rekap

                    ws_rekap.set_column(i, i, width, current_fmt)

            # Write summary sheets
            summary_bansos.to_excel(writer, sheet_name="SUMMARY_BY_BANSOS", index=False)
            summary_desil.to_excel(writer, sheet_name="SUMMARY_BY_DESIL", index=False)

            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 0)

                # Get the DataFrame for this sheet (re-cleaned needed to match columns)
                # We just need columns list to know what to apply
                # But we can't easily get the modified DF back from writer
                # So we infer from original DF + drop logic

                original_df = None
                if sheet_name == "KELUARGA_MASTER": original_df = keluarga_master
                elif sheet_name == "ANGGOTA_MASTER": original_df = anggota_master
                elif sheet_name in desil_sheets: original_df = desil_sheets[sheet_name]
                elif sheet_name == "PKH_DETAIL": original_df = pkh
                elif sheet_name == "BPNT_DETAIL": original_df = bpnt
                elif sheet_name == "PBI_DETAIL": original_df = pbi
                elif sheet_name == "ASET_DETAIL": original_df = aset
                elif sheet_name == "SUMMARY_BY_BANSOS": original_df = summary_bansos
                elif sheet_name == "SUMMARY_BY_DESIL": original_df = summary_desil

                if original_df is not None and not original_df.empty:
                    # Simulate the prepared columns
                    temp_cols = [c for c in original_df.columns if c not in cols_to_drop]

                    # Rewrite header
                    for col_num, col_name in enumerate(temp_cols):
                        ws.write(0, col_num, col_name, header_fmt)

                        # Apply column format
                        # Text
                        if col_name in text_cols or 'nik' in col_name.lower() or 'no_kk' in col_name.lower():
                            ws.set_column(col_num, col_num, 25, text_fmt)
                        # Date
                        elif col_name in date_cols or 'tanggal' in col_name.lower() or 'tgl' in col_name.lower():
                             ws.set_column(col_num, col_num, 15, date_fmt)
                        else:
                             # Auto-fit estimate
                             max_len = max(original_df[col_name].astype(str).map(len).max() if not original_df.empty else 0, len(str(col_name))) + 2
                             ws.set_column(col_num, col_num, min(max_len, 60))

                    # Row striping
                    # Note: xlsxwriter doesn't support conditional formatting for existing cells easily without overwrite
                    # But since we just wrote data, we can't re-write row styles without re-writing data
                    # or using conditional formatting ranges.
                    # Simplified: Skip row striping to prioritize cell formats (Text/Date),
                    # OR use conditional formatting for striping which overlays.

                    ws.conditional_format(1, 0, min(len(original_df)+1, 10000), len(temp_cols)-1,
                                          {'type': 'formula', 'criteria': '=MOD(ROW(),2)=1', 'format': odd_fmt})
                    ws.conditional_format(1, 0, min(len(original_df)+1, 10000), len(temp_cols)-1,
                                          {'type': 'formula', 'criteria': '=MOD(ROW(),2)=0', 'format': even_fmt})

            # VISUALIZATIONS sheet
            if charts:
                vis_ws = workbook.add_worksheet("VISUALIZATIONS")
                writer.sheets["VISUALIZATIONS"] = vis_ws
                vis_ws.set_column(0, 6, 40)

                row = 0
                for key, path in charts.items():
                    try:
                        vis_ws.write(row, 0, key.replace("_", " ").upper(), header_fmt)
                        vis_ws.insert_image(row + 1, 0, path, {"x_scale": 0.9, "y_scale": 0.9})
                        row += 20
                    except Exception as e:
                        vis_ws.write(row, 0, f"Failed to insert {key}: {e}")
                        row += 2

        return output.getvalue()

    def build_pdfs(self, files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """Build multiple PDF reports split by Desil groups"""
        print("[PDF] Building PDF reports...")

        # Load all master data
        keluarga_master = self.build_keluarga_master(files_dict)
        anggota_master = self.build_anggota_master(files_dict)

        if keluarga_master.empty:
            return {}

        # Ensure desil_class exists
        if "desil_class" not in keluarga_master.columns:
            if "desil_nasional" in keluarga_master.columns:
                 keluarga_master["desil_class"] = keluarga_master["desil_nasional"].apply(map_desil)
            elif "desil" in keluarga_master.columns:
                 keluarga_master["desil_class"] = keluarga_master["desil"].apply(map_desil)
            else:
                 keluarga_master["desil_class"] = "DESIL_BELUM_DITENTUKAN"

        # Split by Desil
        groups = keluarga_master.groupby("desil_class")

        pdf_files = {}

        # 1. Generate FULL REPORT (All Families)
        print("  - Generating FULL REPORT (All Families)...")
        try:
            # Reuse visualization logic for full dataset
            # Note: create_visualizations might overwrite files in tmpdir, but since we read them immediately into PDF it's fine
            # or we can generate specific charts for full report.
            # For simplicity, we use the charts generated for the whole dataset if available or generate new ones.

            # We need to pass the full dataset to create_visualizations
            # Currently it uses files_dict but we can pass dataframes.
            # Refactor create_visualizations to accept DFs optionally?
            # Yes, I already updated create_visualizations to accept keluarga_master and anggota_master.

            charts_full = self.create_visualizations(files_dict, keluarga_master, anggota_master)
            pdf_full = self.generate_single_pdf("FULL REPORT", keluarga_master, anggota_master, files_dict, charts_full)
            pdf_files["FULL_REPORT.pdf"] = pdf_full
        except Exception as e:
            print(f"Error generating FULL REPORT: {e}")

        # 2. Generate Per-Desil Reports
        for desil_name, group_df in groups:
            if group_df.empty:
                continue

            print(f"  - Generating report for {desil_name} ({len(group_df)} families)...")

            # Filter members
            valid_ids = set(group_df["id_keluarga"].astype(str))
            group_members = pd.DataFrame()
            if not anggota_master.empty:
                # Ensure we match string types
                if "id_keluarga" in anggota_master.columns:
                    mask = anggota_master["id_keluarga"].astype(str).isin(valid_ids)
                    group_members = anggota_master[mask].copy()
                elif "id_keluarga_parent" in anggota_master.columns:
                    mask = anggota_master["id_keluarga_parent"].astype(str).isin(valid_ids)
                    group_members = anggota_master[mask].copy()

            # Generate charts for this specific group
            charts = self.create_visualizations(files_dict, group_df, group_members)

            # Build PDF for this group
            pdf_bytes = self.generate_single_pdf(desil_name, group_df, group_members, files_dict, charts)

            # Clean filename
            safe_name = desil_name.replace(" ", "_").upper()
            pdf_files[f"{safe_name}_REPORT.pdf"] = pdf_bytes

        return pdf_files

    def generate_single_pdf(
        self,
        title_suffix: str,
        families: pd.DataFrame,
        members: pd.DataFrame,
        files_dict: Dict[str, bytes],
        charts: Dict[str, str]
    ) -> bytes:
        """
        Generate a premium PDF report for families with 7 data sections.

        PREMIUM DESIGN FEATURES:
        - F4 page size (21.5 x 33 cm)
        - Professional slate/blue color scheme (#2980B9, #2C3E50)
        - Logo integration in header (assets/icon.png)
        - Enhanced footer with page number, timestamp, and CONFIDENTIAL watermark
        - Alternating row colors (#F4F6F7) for better readability
        - Formatted currency values (Rp. 1.500.000)
        - 7 sections: Family, Members, Aset Tidak Bergerak, Aset Bergerak, PBI, BPNT, PKH
        """
        output = io.BytesIO()
        PAGE_SIZE = PAGE_SIZE_F4  # F4: 21.5 x 33 cm

        # -- Premium Footer & Header Canvas --
        def add_header_footer(canvas, doc):
            canvas.saveState()

            # Footer with premium styling
            canvas.setFont('Helvetica', 8)
            canvas.setFillColor(colors.HexColor("#2C3E50"))
            page_num = canvas.getPageNumber()

            # Left: Page number
            canvas.drawString(1.5*cm, 1*cm, f"Page {page_num}")

            # Center: Generation timestamp
            timestamp_text = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            timestamp_width = canvas.stringWidth(timestamp_text, 'Helvetica', 8)
            canvas.drawString((PAGE_SIZE[0] - timestamp_width) / 2, 1*cm, timestamp_text)

            # Right: CONFIDENTIAL watermark
            canvas.drawRightString(PAGE_SIZE[0] - 1.5*cm, 1*cm, "CONFIDENTIAL")

            # Footer line
            canvas.setStrokeColor(colors.HexColor("#2980B9"))
            canvas.setLineWidth(0.5)
            canvas.line(1.5*cm, 1.4*cm, PAGE_SIZE[0]-1.5*cm, 1.4*cm)

            # Header Logo (if exists)
            logo_path = os.path.join("assets", "icon_p.png")
            if os.path.exists(logo_path):
                try:
                    # Draw logo at top left corner - Adjusted for wide image (902x309)
                    # Increased width to 4.5cm, adjusted Y position
                    canvas.drawImage(logo_path, 1.5*cm, PAGE_SIZE[1] - 2.2*cm,
                                   width=4.5*cm, height=1.5*cm, mask='auto', preserveAspectRatio=True)
                except Exception as e:
                    pass  # Silently fail if logo can't be loaded

            canvas.restoreState()

        doc = SimpleDocTemplate(
            output,
            pagesize=PAGE_SIZE,
            leftMargin=1.5 * cm,
            rightMargin=1.5 * cm,
            topMargin=2.5 * cm,  # Reduced from 3.0 to 2.5 cm
            bottomMargin=2.0 * cm,
        )

        styles = getSampleStyleSheet()

        # Premium Custom Styles with slate/blue theme
        style_title = ParagraphStyle('CustomTitle',
            parent=styles['Title'],
            fontSize=22,
            textColor=colors.HexColor("#2C3E50"),
            spaceAfter=20,
            alignment=1,
            fontName='Helvetica-Bold'
        )
        style_heading2 = ParagraphStyle('CustomH2',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.white,
            backColor=colors.HexColor("#2C3E50"),
            borderPadding=8,
            spaceBefore=15,
            spaceAfter=10,
            keepWithNext=True,
            fontName='Helvetica-Bold'
        )
        style_heading3 = ParagraphStyle('CustomH3',
            parent=styles['Heading3'],
            fontSize=12,
            textColor=colors.HexColor("#2980B9"),
            spaceBefore=10,
            spaceAfter=6,
            keepWithNext=True,
            fontName='Helvetica-Bold'
        )
        style_normal = ParagraphStyle('CustomNormal',
            parent=styles['BodyText'],
            fontSize=10,
            leading=12
        )
        small_style = ParagraphStyle('SmallBody',
            parent=styles['BodyText'],
            fontSize=8,
            leading=10,
            alignment=1
        )
        small_style_left = ParagraphStyle('SmallBodyLeft',
            parent=styles['BodyText'],
            fontSize=8,
            leading=10,
            alignment=0
        )

        story = []
        page_width = PAGE_SIZE[0] - doc.leftMargin - doc.rightMargin

        # Helper functions for premium formatting
        def safe_text(value: Any) -> str:
            """Safely convert any value to text, handling None and NaN"""
            if value is None: return "-"
            if isinstance(value, float) and pd.isna(value): return "-"
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none", "null", ""}: return "-"
            return text

        def fmt_rupiah(value: Any) -> str:
            """Format currency values in Indonesian Rupiah format (Rp. 1.500.000)"""
            val_str = safe_text(value)
            if val_str == "-": return "-"
            try:
                # Clean the value
                clean_val = val_str.replace("Rp", "").replace(".", "").replace(",", "").replace(" ", "").strip()
                if not clean_val or clean_val == "0":
                    return "-"
                num = float(clean_val)
                # Format with thousand separators using dots
                formatted = f"Rp. {int(num):,}".replace(",", ".")
                return formatted
            except:
                return val_str

        def fmt_date(value: Any) -> str:
            """Format date values consistently"""
            val_str = safe_text(value)
            if val_str == "-": return "-"
            try:
                # Try to parse and reformat dates
                from datetime import datetime as dt
                if "/" in val_str:
                    parsed = dt.strptime(val_str, "%d/%m/%Y")
                    return parsed.strftime("%d-%m-%Y")
                elif "-" in val_str and len(val_str) == 10:
                    return val_str  # Already in good format
                return val_str
            except:
                return val_str

        def pick_value(row, candidates: List[str], fmt_func=None) -> str:
            """Pick first available value from candidate columns (works with dict or Series)"""
            for col in candidates:
                if col in row:  # Works for both dict and Series
                    val = row[col]
                    if not (isinstance(val, float) and pd.isna(val)):
                        txt = safe_text(val)
                        if txt != "-" and fmt_func:
                            return fmt_func(txt)
                        return txt
            return "-"

        # Load bansos details
        pkh = pd.read_csv(io.BytesIO(files_dict.get("pkh_raw.csv", b""))) if "pkh_raw.csv" in files_dict else pd.DataFrame()
        bpnt = pd.read_csv(io.BytesIO(files_dict.get("bpnt_raw.csv", b""))) if "bpnt_raw.csv" in files_dict else pd.DataFrame()
        pbi = pd.read_csv(io.BytesIO(files_dict.get("pbi_raw.csv", b""))) if "pbi_raw.csv" in files_dict else pd.DataFrame()
        aset_merged = pd.read_csv(io.BytesIO(files_dict.get("aset_merged.csv", b""))) if "aset_merged.csv" in files_dict else pd.DataFrame()

        # --- COVER PAGE ---
        story.append(Paragraph(f"DTSEN Report - {title_suffix}", style_title))
        story.append(Spacer(1, 0.2 * inch))

        # Summary Table with premium colors
        summary_data = [
            ["Metric", "Value"],
            ["Group", title_suffix],
            ["Total Families", str(len(families))],
            ["Total Members", str(len(members))],
            ["Generated At", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ]
        t_summary = Table(summary_data, colWidths=[page_width * 0.4, page_width * 0.6])
        t_summary.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2980B9")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
            ('ROWBACKGROUNDS', (1, 0), (-1, -1), [colors.white, colors.HexColor("#ECF0F1")]),
        ]))
        story.append(t_summary)
        story.append(Spacer(1, 0.3 * inch))

        # === WELFARE SUMMARY CARD DASHBOARD ===
        # Enhanced layout with all new visualizations from a.txt

        # Section 1: DEMOGRAPHICS & POPULATION STRUCTURE
        story.append(Paragraph("<b>1. Demographics & Population Structure</b>", style_heading3))
        story.append(Spacer(1, 0.1 * inch))

        demo_row = []
        if "population_pyramid" in charts:
            demo_row.append(Image(charts["population_pyramid"], width=3.5*inch, height=3.5*inch))
        if "household_size" in charts:
            demo_row.append(Image(charts["household_size"], width=3.5*inch, height=2.8*inch))

        if demo_row:
            t_demo = Table([demo_row], colWidths=[page_width/len(demo_row)]*len(demo_row))
            t_demo.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
            story.append(t_demo)
            story.append(Spacer(1, 0.2 * inch))

        demo_row2 = []
        if "head_of_household" in charts:
            demo_row2.append(Image(charts["head_of_household"], width=3.2*inch, height=3.2*inch))
        if "gender_pie" in charts:
            demo_row2.append(Image(charts["gender_pie"], width=3.2*inch, height=3.2*inch))

        if demo_row2:
            t_demo2 = Table([demo_row2], colWidths=[page_width/len(demo_row2)]*len(demo_row2))
            t_demo2.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
            story.append(t_demo2)
            story.append(Spacer(1, 0.3 * inch))

        # Section 2: WELFARE & SOCIAL AID ANALYSIS
        story.append(Paragraph("<b>2. Welfare & Social Aid Analysis</b>", style_heading3))
        story.append(Spacer(1, 0.1 * inch))

        aid_row1 = []
        if "desil_distribution" in charts:
            aid_row1.append(Image(charts["desil_distribution"], width=3.5*inch, height=2.8*inch))
        if "bansos_penetration" in charts:
            aid_row1.append(Image(charts["bansos_penetration"], width=3.5*inch, height=2.8*inch))

        if aid_row1:
            t_aid1 = Table([aid_row1], colWidths=[page_width/len(aid_row1)]*len(aid_row1))
            t_aid1.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
            story.append(t_aid1)
            story.append(Spacer(1, 0.2 * inch))

        aid_row2 = []
        if "bansos_venn" in charts:
            aid_row2.append(Image(charts["bansos_venn"], width=3.2*inch, height=3.2*inch))
        if "desil_venn" in charts:
            aid_row2.append(Image(charts["desil_venn"], width=3.2*inch, height=3.2*inch))

        if aid_row2:
            t_aid2 = Table([aid_row2], colWidths=[page_width/len(aid_row2)]*len(aid_row2))
            t_aid2.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
            story.append(t_aid2)
            story.append(Spacer(1, 0.3 * inch))

        # Page break before next section
        story.append(PageBreak())

        # Section 3: LIVING CONDITIONS & ASSETS
        story.append(Paragraph("<b>3. Living Conditions & Assets</b>", style_heading3))
        story.append(Spacer(1, 0.1 * inch))

        if "housing_quality" in charts:
            story.append(Image(charts["housing_quality"], width=page_width * 0.95, height=3.0*inch))
            story.append(Spacer(1, 0.2 * inch))

        if "asset_ownership_heatmap" in charts:
            story.append(Image(charts["asset_ownership_heatmap"], width=page_width * 0.85, height=3.5*inch))
            story.append(Spacer(1, 0.2 * inch))

        if "sanitation_water" in charts:
            story.append(Image(charts["sanitation_water"], width=page_width * 0.90, height=3.0*inch))
            story.append(Spacer(1, 0.3 * inch))

        # Section 4: GEOSPATIAL & HOTSPOTS
        if "poverty_hotspots" in charts or "age_hist" in charts:
            story.append(Paragraph("<b>4. Geospatial Analysis & Additional Insights</b>", style_heading3))
            story.append(Spacer(1, 0.1 * inch))

            geo_row = []
            if "poverty_hotspots" in charts:
                geo_row.append(Image(charts["poverty_hotspots"], width=3.5*inch, height=3.0*inch))
            if "age_hist" in charts:
                geo_row.append(Image(charts["age_hist"], width=3.5*inch, height=2.5*inch))

            if geo_row:
                t_geo = Table([geo_row], colWidths=[page_width/len(geo_row)]*len(geo_row))
                t_geo.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t_geo)

        story.append(PageBreak())

        # --- FAMILY DETAILS with 7 SECTIONS ---
        family_records = families.to_dict(orient="records")

        for idx, family_row in enumerate(family_records, 1):
            try:
                id_kel = str(family_row.get("id_keluarga") or family_row.get("ID_KELUARGA") or "").strip()
                if id_kel.lower() in {"nan", "none", ""}: continue

                # Filter members
                fam_members = pd.DataFrame()
                if not members.empty:
                    if "id_keluarga" in members.columns:
                        fam_members = members[members["id_keluarga"].astype(str) == id_kel]
                    elif "id_keluarga_parent" in members.columns:
                        fam_members = members[members["id_keluarga_parent"].astype(str) == id_kel]

                # Calculate fields
                family_row["jumlah_anggota_calc"] = str(len(fam_members))

                if not fam_members.empty:
                    # Extract RT/RW from KYC
                    for col in ["no_rt", "no_rt_kyc", "rt_kyc", "rt"]:
                         if col in fam_members.columns:
                             vals = fam_members[col].dropna().astype(str).replace(["", "nan", "None", "-"], pd.NA).dropna()
                             if not vals.empty:
                                 family_row["no_rt"] = vals.iloc[0]
                                 break
                    for col in ["no_rw", "no_rw_kyc", "rw_kyc", "rw"]:
                         if col in fam_members.columns:
                             vals = fam_members[col].dropna().astype(str).replace(["", "nan", "None", "-"], pd.NA).dropna()
                             if not vals.empty:
                                 family_row["no_rw"] = vals.iloc[0]
                                 break

            # === SECTION 1: FAMILY INFO ===
                head_text = f"Family #{idx}: {html.escape(safe_text(family_row.get('nama_kepala_keluarga','-')))}"
                story.append(Paragraph(f"<b>{head_text}</b>", style_heading2))

                f_table_data = []
                for label, keys in FAMILY_HEADERS:
                    val = pick_value(family_row, keys)
                    f_table_data.append([
                        Paragraph(f"<b>{label}</b>", style_normal),
                        Paragraph(val, style_normal)
                    ])

                t_family = Table(f_table_data, colWidths=[page_width * 0.35, page_width * 0.65])
                t_family.setStyle(TableStyle([
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                    ('BACKGROUND', (0, 0), (0, -1), colors.HexColor("#ECF0F1")),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('PADDING', (0, 0), (-1, -1), 4),
                ]))
                story.append(t_family)
                story.append(Spacer(1, 0.15 * inch))

                # === SECTION 2: MEMBERS ===
                story.append(Paragraph("<b>Members</b>", style_heading3))

                if not fam_members.empty:
                    m_header_row = [Paragraph(f"<b>{h[0]}</b>", small_style) for h in MEMBER_HEADERS]
                    m_data = [m_header_row]

                    for _, mem_row in fam_members.iterrows():
                        row_cells = []
                        for label, keys in MEMBER_HEADERS:
                            val = pick_value(mem_row, keys)
                            # Format dates for Tgl Lahir
                            if label == "Tgl Lahir":
                                val = fmt_date(val)
                            row_cells.append(Paragraph(val, small_style_left))
                        m_data.append(row_cells)

                    col_w = page_width / len(MEMBER_HEADERS)
                    t_members = Table(m_data, colWidths=[col_w] * len(MEMBER_HEADERS), repeatRows=1)
                    t_members.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2980B9")),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4F6F7")]),
                    ]))
                    story.append(t_members)
                else:
                    story.append(Paragraph("No members found.", small_style))

                story.append(Spacer(1, 0.15 * inch))

                # Get asset data
                asset_row = None
                if not aset_merged.empty:
                    matches = aset_merged[aset_merged["id_keluarga"].astype(str) == id_kel]
                    if not matches.empty:
                        asset_row = matches.iloc[0]

                # === SECTION 3: ASET TIDAK BERGERAK (Immovable Assets) ===
                story.append(Paragraph("<b>3. Aset Tidak Bergerak</b>", style_heading3))

                MAPPING_IMMOVABLE = [
                    ("Status Penguasaan Bangunan", ["status_penguasaan_bangunan", "status_lahan", "kepemilikan_rumah"]),
                    ("Lantai Terluas", ["jenis_lantai", "lantai_terluas", "lantai"]),
                    ("Dinding Terluas", ["jenis_dinding", "dinding_terluas", "dinding"]),
                    ("Atap Terluas", ["jenis_atap", "atap_terluas", "atap"]),
                    ("Sumber Air Minum", ["sumber_air_minum", "air_minum"]),
                    ("Jarak Sumber Air Limbah", ["jarak_sumber_air_limbah", "jarak_tinja", "jarak_pembuangan"]),
                    ("Sumber Penerangan", ["sumber_penerangan", "penerangan"]),
                    ("Bahan Bakar Utama", ["bahan_bakar_utama", "bahan_bakar_memasak", "bahan_bakar"]),
                    ("Fasilitas BAB", ["fasilitas_bab", "kepemilikan_kamar_mandi"]),
                    ("Jenis Kloset", ["jenis_kloset", "kloset"]),
                    ("Pembuangan Tinja", ["pembuangan_tinja", "tempat_pembuangan_akhir_tinja"]),
                ]

                a1_data = [[Paragraph("<b>Asset Item</b>", style_normal), Paragraph("<b>Value</b>", style_normal)]]

                if asset_row is not None:
                    for label, keys in MAPPING_IMMOVABLE:
                        val = pick_value(asset_row, keys)
                        a1_data.append([Paragraph(label, style_normal), Paragraph(val, style_normal)])
                else:
                    a1_data.append([Paragraph("No data", style_normal), Paragraph("-", style_normal)])

                t_assets1 = Table(a1_data, colWidths=[page_width * 0.6, page_width * 0.4])
                t_assets1.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2980B9")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4F6F7")]),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ]))
                story.append(t_assets1)
                story.append(Spacer(1, 0.15 * inch))

                # === SECTION 4: ASET BERGERAK (Movable Assets) ===
                story.append(Paragraph("<b>4. Aset Bergerak</b>", style_heading3))

                MAPPING_MOVABLE = [
                    ("Jumlah Sapi", ["jml_sapi", "jumlah_sapi", "sapi"]),
                    ("Jumlah Kerbau", ["jml_kerbau", "jumlah_kerbau", "kerbau"]),
                    ("Jumlah Kambing/Domba", ["jml_kambing_domba", "jumlah_kambing", "jumlah_domba", "kambing", "domba"]),
                    ("Jumlah Babi", ["jml_babi", "jumlah_babi", "babi"]),
                    ("Jumlah Kuda", ["jml_kuda", "jumlah_kuda", "kuda"]),
                    ("Air Conditioner (AC)", ["ac", "air_conditioner"]),
                    ("Emas/Perhiasan min 10 gr", ["emas", "perhiasan"]),
                    ("Kapal/Perahu Motor", ["kapal_perahu_motor", "kapal", "perahu_motor"]),
                    ("Komputer/Laptop/Tablet", ["komputer", "laptop", "tablet"]),
                    ("Lemari Es/Kulkas", ["kulkas", "lemari_es"]),
                    ("Mobil", ["mobil"]),
                    ("Pemanas Air (Water Heater)", ["pemanas_air", "water_heater"]),
                    ("Perahu", ["perahu"]),
                    ("Sepeda", ["sepeda"]),
                    ("Sepeda Motor", ["sepeda_motor", "motor"]),
                    ("Smartphone", ["smartphone", "hp"]),
                    ("Tabung Gas 5.5 kg atau lebih", ["tabung_gas"]),
                    ("Telepon Rumah (PSTN)", ["telepon_rumah", "telepon"]),
                    ("Televisi Layar Datar min 30 inch", ["televisi", "tv_flat", "tv"]),
                ]

                header_row = [
                    Paragraph("<b>Asset Item</b>", style_normal),
                    Paragraph("<b>Value</b>", style_normal),
                    Paragraph("<b>Asset Item</b>", style_normal),
                    Paragraph("<b>Value</b>", style_normal),
                ]
                a2_data = [header_row]

                movable_pairs = []
                if asset_row is not None:
                    for label, keys in MAPPING_MOVABLE:
                        val = pick_value(asset_row, keys)
                        movable_pairs.append((
                            Paragraph(label, style_normal),
                            Paragraph(val, style_normal)
                        ))
                else:
                    movable_pairs.append((
                        Paragraph("No data", style_normal),
                        Paragraph("-", style_normal)
                    ))

                for i in range(0, len(movable_pairs), 2):
                    first_label, first_value = movable_pairs[i]
                    if i + 1 < len(movable_pairs):
                        second_label, second_value = movable_pairs[i + 1]
                    else:
                        second_label = Paragraph("", style_normal)
                        second_value = Paragraph("", style_normal)
                    a2_data.append([
                        first_label,
                        first_value,
                        second_label,
                        second_value,
                    ])

                t_assets2 = Table(a2_data, colWidths=[
                    page_width * 0.3,
                    page_width * 0.2,
                    page_width * 0.3,
                    page_width * 0.2,
                ])
                t_assets2.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2980B9")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4F6F7")]),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ]))
                story.append(t_assets2)
                story.append(Spacer(1, 0.15 * inch))

                # Get bansos data for this family
                pkh_rows = pkh[pkh["id_keluarga"].astype(str) == id_kel] if not pkh.empty and "id_keluarga" in pkh.columns else pd.DataFrame()
                bpnt_rows = bpnt[bpnt["id_keluarga"].astype(str) == id_kel] if not bpnt.empty and "id_keluarga" in bpnt.columns else pd.DataFrame()
                pbi_rows = pbi[pbi.get("id_keluarga_parent", pbi.get("id_keluarga", pd.Series())).astype(str) == id_kel] if not pbi.empty else pd.DataFrame()

                # === SECTION 5: PBI INFO ===
                if not pbi_rows.empty:
                    story.append(Paragraph("<b>5. PBI Information</b>", style_heading3))
                    pbi_headers = ["NAMA", "NIK", "PERIODE AWAL", "PERIODE AKHIR"]
                    pbi_data = [[Paragraph(f"<b>{h}</b>", small_style) for h in pbi_headers]]

                    for _, prow in pbi_rows.iterrows():
                        p_nama = pick_value(prow, ["nama", "nama_lengkap", "NAMA", "nama_peserta"])
                        p_nik = pick_value(prow, ["nik", "NIK", "nik_peserta"])
                        p_awal = pick_value(prow, ["periode_awal", "nama_periode"])
                        p_akhir = pick_value(prow, ["periode_akhir"])
                        pbi_data.append([
                            Paragraph(p_nama, small_style_left),
                            Paragraph(p_nik, small_style_left),
                            Paragraph(p_awal, small_style),
                            Paragraph(p_akhir, small_style)
                        ])

                    t_pbi = Table(pbi_data, colWidths=[page_width*0.3, page_width*0.25, page_width*0.225, page_width*0.225])
                    t_pbi.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#27AE60")), # Green
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#E9F7EF")]),
                    ]))
                    story.append(t_pbi)
                    story.append(Spacer(1, 0.15 * inch))

                # === SECTION 6: BPNT INFO ===
                if not bpnt_rows.empty:
                    story.append(Paragraph("<b>6. BPNT Information</b>", style_heading3))
                    bpnt_headers = ["TAHAP/PERIODE", "STATUS", "NOMINAL"]
                    bpnt_data = [[Paragraph(f"<b>{h}</b>", small_style) for h in bpnt_headers]]

                    for _, brow in bpnt_rows.iterrows():
                        b_tahap = pick_value(brow, ["tahap", "periode", "bulan", "nama_periode"])
                        b_status = pick_value(brow, ["status", "keterangan_transaksi", "status_transaksi"])
                        b_nom = pick_value(brow, ["nominal", "jumlah_bantuan", "nominal_bansos"], fmt_rupiah)
                        bpnt_data.append([
                            Paragraph(b_tahap, small_style),
                            Paragraph(b_status, small_style),
                            Paragraph(b_nom, small_style)
                        ])

                    t_bpnt = Table(bpnt_data, colWidths=[page_width*0.4, page_width*0.35, page_width*0.25])
                    t_bpnt.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F39C12")), # Orange
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#FEF9E7")]),
                    ]))
                    story.append(t_bpnt)
                    story.append(Spacer(1, 0.15 * inch))

                # === SECTION 7: PKH INFO ===
                if not pkh_rows.empty:
                    story.append(Paragraph("<b>7. PKH Information</b>", style_heading3))
                    pkh_headers = ["TAHAP", "STATUS", "KOMPONEN", "NOMINAL"]
                    pkh_data = [[Paragraph(f"<b>{h}</b>", small_style) for h in pkh_headers]]

                    for _, krow in pkh_rows.iterrows():
                        k_tahap = pick_value(krow, ["tahap", "periode"])
                        k_status = pick_value(krow, ["status", "status_transaksi"])
                        k_komp = pick_value(krow, ["komponen", "kategori", "jenis_bantuan"])
                        k_nom = pick_value(krow, ["nominal", "jumlah_bantuan", "nominal_bansos"], fmt_rupiah)
                        pkh_data.append([
                            Paragraph(k_tahap, small_style),
                            Paragraph(k_status, small_style),
                            Paragraph(k_komp, small_style),
                            Paragraph(k_nom, small_style)
                        ])

                    t_pkh = Table(pkh_data, colWidths=[page_width*0.2, page_width*0.3, page_width*0.3, page_width*0.2])
                    t_pkh.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#8E44AD")), # Purple
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4ECF7")]),
                    ]))
                    story.append(t_pkh)
                    story.append(Spacer(1, 0.15 * inch))

                story.append(PageBreak())

            except Exception as e:
                print(f"Error creating PDF page for family {idx}: {e}")
                continue

        # Build PDF with header/footer
        doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
        return output.getvalue()

    def run_full_pipeline(self, entity_lines: List[str]) -> Dict[str, Any]:
        tx_id = f"TX-{secrets.token_hex(5).upper()}"
        try:
            # Step 1: Fetch families
            families = []
            if HTTPX_AVAILABLE:
                try:
                    import asyncio
                    # Run async fetch for families
                    families = asyncio.run(self.fetch_families_async(entity_lines))
                except Exception as e:
                    print(f"[WARN] Async families fetch failed: {e}, falling back to sync")
                    self._update_progress("[INFO] Fetching families", 0, len(entity_lines))
                    families = self.fetch_families(entity_lines)
            else:
                self._update_progress("[INFO] Fetching families", 0, len(entity_lines))
                families = self.fetch_families(entity_lines)

            if not families:
                raise ValueError("No families fetched")

            id_list = [
                f.get("id_keluarga_parent") or f.get("id_keluarga") or f.get("ID_KELUARGA")
                for f in families
            ]
            id_list = [i for i in id_list if i]

            endpoints = {
                "members": URL_MEMBERS,
                "kyc": URL_KYC,
                "pbi": URL_PBI,
                "pkh": URL_PKH,
                "bpnt": URL_BPNT,
                "aset": URL_ASET,
                "asetb": URL_ASET_BERGERAK,
            }

            files_dict = {}

            # Save families CSV
            families_df = pd.DataFrame(families)
            families_csv = families_df.to_csv(index=False).encode()
            files_dict["families_raw.csv"] = families_csv

            # Fetch other endpoints - CONCURRENTLY if possible
            if HTTPX_AVAILABLE:
                try:
                    import asyncio
                    print("[INFO] Starting concurrent scraping for all endpoints...")

                    # Initialize concurrent progress
                    self.concurrent_total = len(id_list) * len(endpoints)
                    self.concurrent_current = 0

                    async def fetch_all_concurrently():
                        tasks = []
                        labels = []
                        for label, url in endpoints.items():
                            labels.append(label)
                            # Use async path with bounded concurrency for all endpoints
                            tasks.append(self.fetch_endpoint_async(label, url, id_list, use_concurrent_progress=True))
                        results = await asyncio.gather(*tasks)
                        return dict(zip(labels, results))

                    # Execute concurrent fetch
                    results_map = asyncio.run(fetch_all_concurrently())

                    # Process results
                    for label, data in results_map.items():
                        df = pd.DataFrame(data) if data else pd.DataFrame(columns=["id_keluarga_parent"])
                        csv_bytes = df.to_csv(index=False).encode()
                        files_dict[f"{label}_raw.csv"] = csv_bytes

                except Exception as e:
                    print(f"[WARN] Concurrent scraping failed: {e}, falling back to sync")
                    # Fallback to sync loop
                    for label, url in endpoints.items():
                        self._update_progress(f"[FETCH] Fetching {label}", 0, len(id_list))
                        data = self.fetch_endpoint(label, url, id_list)
                        df = pd.DataFrame(data) if data else pd.DataFrame(columns=["id_keluarga_parent"])
                        files_dict[f"{label}_raw.csv"] = df.to_csv(index=False).encode()
            else:
                # Sync loop
                for label, url in endpoints.items():
                    self._update_progress(f"[FETCH] Fetching {label}", 0, len(id_list))
                    data = self.fetch_endpoint(label, url, id_list)

                    df = pd.DataFrame(data) if data else pd.DataFrame(columns=["id_keluarga_parent"])
                    csv_bytes = df.to_csv(index=False).encode()
                    filename = f"{label}_raw.csv"
                    files_dict[filename] = csv_bytes

            # Build aset_clean and aset_merged
            self._update_progress("[BUILD] Building aset_clean", 0, 100)
            aset_raw_data = pd.read_csv(io.BytesIO(files_dict.get("aset_raw.csv", b""))) if "aset_raw.csv" in files_dict else pd.DataFrame()
            aset_df = self.clean_aset(aset_raw_data.to_dict('records') if not aset_raw_data.empty else [])
            files_dict["aset_clean.csv"] = aset_df.to_csv(index=False).encode()

            asetb_raw_data = pd.read_csv(io.BytesIO(files_dict.get("asetb_raw.csv", b""))) if "asetb_raw.csv" in files_dict else pd.DataFrame()
            asetb_df = self.clean_aset_bergerak(asetb_raw_data.to_dict('records') if not asetb_raw_data.empty else [])
            aset_merged_df = aset_df.merge(asetb_df, on="id_keluarga", how="outer") if not asetb_df.empty else aset_df
            files_dict["aset_merged.csv"] = aset_merged_df.to_csv(index=False).encode()

            # Build XLSX (with enhanced features)
            self._update_progress("[DATA] Building XLSX", 0, 100)
            xlsx_bytes = self.build_xlsx(files_dict)
            files_dict["Rekapitulasi.xlsx"] = xlsx_bytes

            # Build PDF (Multiple Files)
            self._update_progress("[PDF] Building PDFs", 0, 100)
            pdf_files = self.build_pdfs(files_dict)
            files_dict.update(pdf_files)

            return {
                "tx_id": tx_id,
                "files": files_dict
            }
        finally:
            # Ensure persistent HTTP client is closed
            self._close_client()

if __name__ == "__main__":
    # Test mode
    pass
