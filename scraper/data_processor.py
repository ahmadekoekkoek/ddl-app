"""
Data Processor
Handles data cleaning, merging, and transformations.
"""

import re
import io
from typing import Dict, List, Any, Optional
from datetime import datetime

import pandas as pd

from core import get_logger, DataOptimizer
from .constants import ASSET_ALIASES, ASSET_IMMOVABLE, ASSET_MOVABLE


class DataProcessor:
    """Cleans, merges, and transforms raw scraped data."""

    def __init__(self):
        self._logger = get_logger('data_processor')

    # Age and date helpers

    @staticmethod
    def compute_age(date_str: str) -> Optional[int]:
        """Compute age from date string."""
        if not date_str or pd.isna(date_str):
            return None
        date_str = str(date_str).strip()

        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d"):
            try:
                dob = datetime.strptime(date_str[:10], fmt)
                today = datetime.now()
                age = today.year - dob.year
                if (today.month, today.day) < (dob.month, dob.day):
                    age -= 1
                return age
            except:
                continue
        return None

    @staticmethod
    def map_desil(val: Any) -> str:
        """Map desil values to legacy labels."""
        try:
            if pd.isna(val) or str(val).strip() in ("", "0", "nan", "None", "-"):
                return "DESIL_BELUM_DITENTUKAN"
            s = str(val).strip()
            if s.isdigit():
                n = int(s)
                if 1 <= n <= 5:
                    return f"DESIL_{n}"
                if 6 <= n <= 10:
                    return "DESIL_6_10"
            return "DESIL_BELUM_DITENTUKAN"
        except Exception:
            return "DESIL_BELUM_DITENTUKAN"

    @staticmethod
    def make_bansos_combo(row: pd.Series) -> str:
        """Create legacy bansos combination string using underscore separator."""

        def _has_flag(names: List[str]) -> bool:
            for name in names:
                try:
                    if bool(row.get(name)):
                        return True
                except Exception:
                    continue
            return False

        has_pkh = _has_flag(["has_pkh", "pkh_flag", "PKH"])
        has_bpnt = _has_flag(["has_bpnt", "bpnt_flag", "BPNT"])
        has_pbi = _has_flag(["has_pbi", "pbi_flag", "PBI"])

        parts: List[str] = []
        if has_pkh:
            parts.append("PKH")
        if has_bpnt:
            parts.append("BPNT")
        if has_pbi:
            parts.append("PBI")
        return "_".join(parts) if parts else "NO_BANSOS"

    @staticmethod
    def pick_value(row: Any, keys: List[str], default: str = "-") -> str:
        """Pick first available value from a list of keys."""
        if isinstance(row, dict):
            for k in keys:
                if k in row and row[k] not in (None, "", "nan", "None"):
                    return str(row[k])
        elif hasattr(row, "__getitem__"):
            for k in keys:
                try:
                    v = row[k]
                    if v not in (None, "", "nan", "None") and not pd.isna(v):
                        return str(v)
                except:
                    continue
        return default

    # Asset cleaning

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

    def normalize_asset_column(self, col: str) -> str:
        """Normalize asset column name using aliases."""
        clean = re.sub(r'[^a-z0-9]', '', col.lower())
        return ASSET_ALIASES.get(clean, col)

    # Master sheet builders

    def build_keluarga_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Build master family sheet from raw data files."""
        self._logger.debug("Building KELUARGA_MASTER")

        # Load data
        families = self._load_csv(files_dict.get("families_raw.csv"))
        pkh = self._load_csv(files_dict.get("pkh_raw.csv"))
        bpnt = self._load_csv(files_dict.get("bpnt_raw.csv"))
        pbi = self._load_csv(files_dict.get("pbi_raw.csv"))
        aset_merged = self._load_csv(files_dict.get("aset_merged.csv"))

        if families.empty:
            self._logger.warning("No family data found")
            return pd.DataFrame()

        # Standardize ID column
        families = self._standardize_id(families)

        # --- Calculate Member Count (jumlah_anggota_calc) ---
        members_raw = self._load_csv(files_dict.get("members_raw.csv"))
        if not members_raw.empty:
            members_raw = self._standardize_id(members_raw)
            if "id_keluarga" in members_raw.columns:
                # Group by family ID and count
                member_counts = members_raw.groupby("id_keluarga").size().reset_index(name="jumlah_anggota_calc")
                member_counts["id_keluarga"] = member_counts["id_keluarga"].astype(str)

                # Merge counts into family DataFrame
                families = families.merge(member_counts, on="id_keluarga", how="left")

                # Fill NaN with 0 or existing jumlah_anggota if available
                if "jumlah_anggota_calc" in families.columns:
                    families["jumlah_anggota_calc"] = families["jumlah_anggota_calc"].fillna(0).astype(int)

        # Fallback if calculation failed or members empty
        if "jumlah_anggota_calc" not in families.columns:
             if "jumlah_anggota" in families.columns:
                 families["jumlah_anggota_calc"] = families["jumlah_anggota"].fillna(0)
             else:
                 families["jumlah_anggota_calc"] = 0

        # NOTE: KYC is NOT merged into KELUARGA_MASTER (legacy behavior)
        # KYC is merged into ANGGOTA_MASTER via idsemesta

        # Add bansos flags
        families["pkh_flag"] = False
        families["bpnt_flag"] = False
        families["pbi_flag"] = False

        if not pkh.empty:
            pkh = self._standardize_id(pkh)
            pkh_ids = set(pkh["id_keluarga"].astype(str))
            families["pkh_flag"] = families["id_keluarga"].astype(str).isin(pkh_ids)

        if not bpnt.empty:
            bpnt = self._standardize_id(bpnt)
            bpnt_ids = set(bpnt["id_keluarga"].astype(str))
            families["bpnt_flag"] = families["id_keluarga"].astype(str).isin(bpnt_ids)

        if not pbi.empty:
            pbi = self._standardize_id(pbi)
            pbi_ids = set(pbi["id_keluarga"].astype(str))
            families["pbi_flag"] = families["id_keluarga"].astype(str).isin(pbi_ids)

        # Legacy-compatible bansos fields
        families["has_pkh"] = families["pkh_flag"]
        families["has_bpnt"] = families["bpnt_flag"]
        families["has_pbi"] = families["pbi_flag"]

        families["bansos_combo"] = families.apply(self.make_bansos_combo, axis=1)

        # Map desil
        desil_col = None
        for col in ["desil_nasional", "desil", "DESIL"]:
            if col in families.columns:
                desil_col = col
                break
        if desil_col:
            families["desil_class"] = families[desil_col].apply(self.map_desil)
        else:
            families["desil_class"] = "DESIL_BELUM_DITENTUKAN"

        # Merge assets (legacy behavior)
        if not aset_merged.empty and "id_keluarga" in aset_merged.columns:
            aset_merged["id_keluarga"] = aset_merged["id_keluarga"].astype(str)
            families = families.merge(aset_merged, on="id_keluarga", how="left", suffixes=("", "_aset"))

        # Optimize memory
        families = DataOptimizer.optimize_dataframe(families)

        self._logger.info(f"Built KELUARGA_MASTER with {len(families)} rows")
        return families

    def build_anggota_master(self, files_dict: Dict[str, bytes]) -> pd.DataFrame:
        """Build master member sheet from raw data files."""
        self._logger.debug("Building ANGGOTA_MASTER")

        members = self._load_csv(files_dict.get("members_raw.csv"))
        families = self._load_csv(files_dict.get("families_raw.csv"))
        kyc = self._load_csv(files_dict.get("kyc_raw.csv"))
        pkh = self._load_csv(files_dict.get("pkh_raw.csv"))
        bpnt = self._load_csv(files_dict.get("bpnt_raw.csv"))
        pbi = self._load_csv(files_dict.get("pbi_raw.csv"))

        family_context = None
        if not families.empty:
            families = self._standardize_id(families)

            # Build bansos flags (same as in build_keluarga_master)
            families["has_pkh"] = False
            families["has_bpnt"] = False
            families["has_pbi"] = False

            if not pkh.empty:
                pkh = self._standardize_id(pkh)
                pkh_ids = set(pkh["id_keluarga"].astype(str))
                families["has_pkh"] = families["id_keluarga"].astype(str).isin(pkh_ids)

            if not bpnt.empty:
                bpnt = self._standardize_id(bpnt)
                bpnt_ids = set(bpnt["id_keluarga"].astype(str))
                families["has_bpnt"] = families["id_keluarga"].astype(str).isin(bpnt_ids)

            if not pbi.empty:
                pbi = self._standardize_id(pbi)
                pbi_ids = set(pbi["id_keluarga"].astype(str))
                families["has_pbi"] = families["id_keluarga"].astype(str).isin(pbi_ids)

            families["bansos_combo"] = families.apply(self.make_bansos_combo, axis=1)

            # Map desil
            desil_col = None
            for col in ["desil_nasional", "desil", "DESIL"]:
                if col in families.columns:
                    desil_col = col
                    break
            if desil_col:
                families["desil_class"] = families[desil_col].apply(self.map_desil)
            else:
                families["desil_class"] = "DESIL_BELUM_DITENTUKAN"

            if "id_keluarga" in families.columns:
                family_context = families[["id_keluarga", "desil_class", "bansos_combo"]].copy()

        if members.empty:
            self._logger.warning("No member data found")
            return pd.DataFrame()

        members = self._standardize_id(members)

        # Merge KYC via idsemesta (like legacy version)
        if not kyc.empty and "idsemesta" in members.columns and "idsemesta" in kyc.columns:
            members = members.merge(kyc, on="idsemesta", how="left", suffixes=("", "_kyc"))

        # Compute age (use 'age' for legacy compatibility)
        for col in ["tgl_lahir", "tanggal_lahir", "TGL_LAHIR"]:
            if col in members.columns:
                members["age"] = members[col].apply(self.compute_age)
                break
        else:
            members["age"] = None

        # Clean gender
        for col in ["jenis_kelamin", "jenkel", "id_jenis_kelamin"]:
            if col in members.columns:
                members["gender_clean"] = members[col].apply(self._clean_gender)
                break
        else:
            members["gender_clean"] = "-"

        # Attach desil/bansos from family context if available
        if family_context is not None and "id_keluarga" in members.columns:
            members = members.merge(family_context, on="id_keluarga", how="left")

        # Merge PBI data via NIK (like legacy version)
        if not pbi.empty:
            pbi_nik_col = next((c for c in ["nik", "NIK", "nik_input"] if c in pbi.columns), None)
            member_nik_col = next((c for c in ["nik", "NIK"] if c in members.columns), None)
            if pbi_nik_col and member_nik_col:
                # Rename PBI columns to avoid conflicts
                pbi_renamed = pbi.rename(columns={
                    "nama": "nama_pbi",
                    "nik": "nik_pbi" if "nik" in pbi.columns else pbi_nik_col
                })
                members = members.merge(
                    pbi_renamed,
                    left_on=member_nik_col,
                    right_on=pbi_nik_col if pbi_nik_col != "nik" else "nik_pbi",
                    how="left",
                    suffixes=("", "_pbi")
                )

        members = DataOptimizer.optimize_dataframe(members)

        self._logger.info(f"Built ANGGOTA_MASTER with {len(members)} rows")
        return members

    def build_desil_sheets(self, keluarga_master: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Build per-desil breakdown sheets."""
        labels = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
        sheets: Dict[str, pd.DataFrame] = {}

        for label in labels:
            if not keluarga_master.empty and "desil_class" in keluarga_master.columns:
                sheets[label] = keluarga_master[keluarga_master["desil_class"] == label].copy()
            else:
                sheets[label] = pd.DataFrame()

        return sheets

    # Helper methods

    def _load_csv(self, data: Optional[bytes]) -> pd.DataFrame:
        """Load CSV from bytes."""
        if not data:
            return pd.DataFrame()
        try:
            return pd.read_csv(io.BytesIO(data))
        except Exception as e:
            self._logger.warning(f"Failed to load CSV: {e}")
            return pd.DataFrame()

    def _standardize_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize id_keluarga column."""
        if df.empty:
            return df

        for col in ["id_keluarga_parent", "id_keluarga", "ID_KELUARGA"]:
            if col in df.columns:
                df["id_keluarga"] = df[col].astype(str)
                break
        else:
            df["id_keluarga"] = ""

        return df

    def _clean_gender(self, val: Any) -> str:
        """Clean gender value."""
        if pd.isna(val):
            return "-"
        s = str(val).strip().upper()
        if s in ("1", "L", "LAKI-LAKI", "LAKI", "M", "MALE"):
            return "Laki-laki"
        elif s in ("2", "P", "PEREMPUAN", "WANITA", "F", "FEMALE"):
            return "Perempuan"
        return s if s else "-"

    def merge_asset_data(self, aset_df: pd.DataFrame, asetb_df: pd.DataFrame) -> pd.DataFrame:
        """Merge immovable and movable asset dataframes."""
        if aset_df.empty and asetb_df.empty:
            return pd.DataFrame(columns=["id_keluarga"])

        if aset_df.empty:
            return asetb_df
        if asetb_df.empty:
            return aset_df

        return aset_df.merge(asetb_df, on="id_keluarga", how="outer")

    def process_raw_data(self, files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """Process all raw data files and add computed files."""
        self._logger.info("Processing raw data files")

        # Clean and merge assets
        aset_raw = self._load_csv(files_dict.get("aset_raw.csv"))
        asetb_raw = self._load_csv(files_dict.get("asetb_raw.csv"))

        aset_clean = self.clean_aset(aset_raw.to_dict('records') if not aset_raw.empty else [])
        asetb_clean = self.clean_aset_bergerak(asetb_raw.to_dict('records') if not asetb_raw.empty else [])
        aset_merged = self.merge_asset_data(aset_clean, asetb_clean)

        files_dict["aset_clean.csv"] = aset_clean.to_csv(index=False).encode()
        files_dict["aset_merged.csv"] = aset_merged.to_csv(index=False).encode()

        self._logger.info("Raw data processing complete")
        return files_dict
