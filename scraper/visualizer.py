"""
Visualizer
Creates charts and visualizations with fallback strategies.
"""

import io
import re
import tempfile
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd

from core import get_logger

# Check for matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib_venn import venn2, venn3
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None


class Visualizer:
    """Creates charts and visualizations with multiple fallback strategies."""

    def __init__(self):
        self._logger = get_logger('visualizer')
        self.temp_dir = tempfile.mkdtemp()

    def _legacy_map_desil(self, val: Any) -> str:
        try:
            if pd.isna(val) or str(val).strip() in ("", "0", "nan", "None", "-"):
                return "DESIL_BELUM_DITENTUKAN"
            s = str(val).strip()
            digits = re.findall(r"\d+", s)
            if digits:
                n = int(digits[0])
                if 1 <= n <= 5:
                    return f"DESIL_{n}"
                if 6 <= n <= 10:
                    return "DESIL_6_10"
            return "DESIL_BELUM_DITENTUKAN"
        except Exception:
            return "DESIL_BELUM_DITENTUKAN"

    def _clean_gender_simple(self, val: Any) -> str:
        if pd.isna(val):
            return "-"
        s = str(val).strip().upper()
        if s in ("1", "L", "LAKI-LAKI", "LAKI", "M", "MALE"):
            return "Laki-laki"
        if s in ("2", "P", "PEREMPUAN", "WANITA", "F", "FEMALE"):
            return "Perempuan"
        return s or "-"

    def _compute_age(self, val: Any) -> Optional[int]:
        try:
            dt = pd.to_datetime(val, errors='coerce')
            if pd.isna(dt):
                return None
            today = pd.Timestamp.today()
            age = today.year - dt.year - ((today.month, today.day) < (dt.month, dt.day))
            return int(age)
        except Exception:
            return None

    def _make_bansos_combo_fallback(self, row: pd.Series) -> str:
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

    def _prepare_families(self, keluarga_master: pd.DataFrame, files_dict: Optional[Dict[str, bytes]]) -> pd.DataFrame:
        families = keluarga_master.copy() if isinstance(keluarga_master, pd.DataFrame) else pd.DataFrame()
        if families.empty and files_dict:
            families = self._load_csv(files_dict.get("families_raw.csv"))

        if families.empty:
            return families

        if "id_keluarga" in families.columns:
            families["id_keluarga"] = families["id_keluarga"].astype(str)

        if "desil_class" not in families.columns:
            desil_col = next((c for c in ["desil_nasional", "desil", "DESIL"] if c in families.columns), None)
            if desil_col:
                families["desil_class"] = families[desil_col].apply(self._legacy_map_desil)
            else:
                families["desil_class"] = "DESIL_BELUM_DITENTUKAN"
        else:
            families["desil_class"] = families["desil_class"].apply(self._legacy_map_desil)

        for legacy, fallback in [("has_pkh", "pkh_flag"), ("has_bpnt", "bpnt_flag"), ("has_pbi", "pbi_flag")]:
            if legacy not in families.columns and fallback in families.columns:
                families[legacy] = families[fallback]

        for col in ("has_pkh", "pkh_flag", "has_bpnt", "bpnt_flag", "has_pbi", "pbi_flag"):
            if col not in families.columns:
                families[col] = False

        if "bansos_combo" not in families.columns:
            families["bansos_combo"] = families.apply(self._make_bansos_combo_fallback, axis=1)

        return families

    def _prepare_members(self, anggota_master: pd.DataFrame, files_dict: Optional[Dict[str, bytes]],
                         families_df: pd.DataFrame) -> pd.DataFrame:
        members = anggota_master.copy() if isinstance(anggota_master, pd.DataFrame) else pd.DataFrame()
        if members.empty and files_dict:
            members = self._load_csv(files_dict.get("members_raw.csv"))

        if members.empty:
            return members

        if "id_keluarga" in members.columns:
            members["id_keluarga"] = members["id_keluarga"].astype(str)

        if "age" not in members.columns:
            if "usia" in members.columns:
                members["age"] = members["usia"]
            elif "umur" in members.columns:
                members["age"] = members["umur"]
            else:
                dob_col = next((c for c in ["tgl_lahir", "tanggal_lahir", "TGL_LAHIR"] if c in members.columns), None)
                if dob_col:
                    members["age"] = members[dob_col].apply(self._compute_age)

        if "gender_clean" not in members.columns:
            gender_col = next((c for c in ["gender", "jenis_kelamin", "jenkel", "id_jenis_kelamin"] if c in members.columns), None)
            if gender_col:
                members["gender_clean"] = members[gender_col].apply(self._clean_gender_simple)

        if "desil_class" not in members.columns and not families_df.empty and "id_keluarga" in members.columns and "id_keluarga" in families_df.columns:
            members = members.merge(
                families_df[["id_keluarga", "desil_class"]],
                on="id_keluarga",
                how="left"
            )

        return members

    def create_visualizations(self, keluarga_master: pd.DataFrame,
                              anggota_master: pd.DataFrame,
                              files_dict: Dict[str, bytes] = None) -> Dict[str, bytes]:
        """Create all visualizations for reports."""
        charts = {}

        if not MATPLOTLIB_AVAILABLE:
            self._logger.warning("matplotlib not available - skipping visualizations")
            return charts
        families_df = self._prepare_families(keluarga_master, files_dict)
        members_df = self._prepare_members(anggota_master, files_dict, families_df)
        aset_df = self._load_csv(files_dict.get("aset_merged.csv") if files_dict else None)

        if families_df.empty and members_df.empty and aset_df.empty:
            self._logger.warning("No data for visualizations")
            return charts

        try:
            charts.update(self._create_bansos_venn(families_df))
            charts.update(self._create_desil_chart(families_df))
            charts.update(self._create_age_histogram(members_df))
            charts.update(self._create_gender_chart(members_df))
            charts.update(self._create_population_pyramid(members_df))
            charts.update(self._create_household_size(families_df))
            charts.update(self._create_head_of_household_profile(members_df))
            charts.update(self._create_bansos_penetration(families_df))
            charts.update(self._create_desil_venn(files_dict, families_df))
            charts.update(self._create_sanitation_water(aset_df))
            charts.update(self._create_age_by_desil(members_df))
            charts.update(self._create_housing_chart(families_df, files_dict, aset_df))
            charts.update(self._create_asset_heatmap(files_dict, aset_df))
            charts.update(self._create_poverty_hotspots(families_df))

        except Exception as e:
            self._logger.error(f"Visualization creation error: {e}")

        return charts

    def _create_bansos_venn(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create BANSOS Venn diagram."""
        try:
            if df.empty:
                return {}

            # Use has_pkh, has_bpnt, has_pbi flags from data processor
            pkh_col = "has_pkh" if "has_pkh" in df.columns else "pkh_flag"
            bpnt_col = "has_bpnt" if "has_bpnt" in df.columns else "bpnt_flag"
            pbi_col = "has_pbi" if "has_pbi" in df.columns else "pbi_flag"

            pkh_set = set(
                df.loc[df.get(pkh_col, False).fillna(False).astype(bool), "id_keluarga"].astype(str)
            ) if pkh_col in df.columns and "id_keluarga" in df.columns else set()
            bpnt_set = set(
                df.loc[df.get(bpnt_col, False).fillna(False).astype(bool), "id_keluarga"].astype(str)
            ) if bpnt_col in df.columns and "id_keluarga" in df.columns else set()
            pbi_set = set(
                df.loc[df.get(pbi_col, False).fillna(False).astype(bool), "id_keluarga"].astype(str)
            ) if pbi_col in df.columns and "id_keluarga" in df.columns else set()

            if not any([pkh_set, bpnt_set, pbi_set]):
                return {}

            fig, ax = plt.subplots(figsize=(10, 8))

            try:
                venn = venn3([pkh_set, bpnt_set, pbi_set],
                             set_labels=('PKH', 'BPNT', 'PBI'), ax=ax)
                ax.set_title('Distribusi Penerima BANSOS', fontsize=14, fontweight='bold')
            except Exception as e:
                self._logger.warning(f"Venn diagram failed, using fallback: {e}")
                return self._fallback_bar(df, [pkh_col, bpnt_col, pbi_col],
                                         "Distribusi BANSOS", "bansos_bar")

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"bansos_venn": buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"BANSOS venn failed: {e}")
            return {}

    def _create_desil_chart(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create desil distribution bar chart."""
        try:
            if "desil_class" not in df.columns:
                return {}
            desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
            counts = df["desil_class"].value_counts().reindex(desil_order, fill_value=0)
            counts = counts[counts > 0]
            if counts.empty:
                return {}

            fig, ax = plt.subplots(figsize=(12, 6))
            colors = ['#d62728', '#ff7f0e', '#ffbb78', '#98df8a', '#2ca02c']

            bars = ax.bar(range(len(counts)), counts.values, color=colors[:len(counts)])
            ax.set_xticks(range(len(counts)))
            ax.set_xticklabels(counts.index, rotation=45, ha='right')
            ax.set_ylabel('Jumlah Keluarga')
            ax.set_title('Distribusi Keluarga per Kategori DESIL', fontsize=14, fontweight='bold')

            # Add value labels
            for bar, val in zip(bars, counts.values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                       str(val), ha='center', va='bottom', fontsize=10)

            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"desil_distribution": buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"Desil chart failed: {e}")
            return {}

    def _create_age_histogram(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create age distribution histogram."""
        try:
            # Support both 'age' and 'usia' columns
            age_col = None
            for col in ["age", "usia", "umur"]:
                if col in df.columns:
                    age_col = col
                    break

            if not age_col or df.empty:
                return {}

            ages = pd.to_numeric(df[age_col], errors='coerce').dropna()
            if ages.empty:
                return {}

            fig, ax = plt.subplots(figsize=(12, 6))

            bins = [0, 5, 12, 18, 25, 35, 45, 55, 65, 100]
            labels = ['0-5', '6-12', '13-18', '19-25', '26-35', '36-45', '46-55', '56-65', '65+']

            ax.hist(ages, bins=bins, edgecolor='black', alpha=0.7, color='steelblue')
            ax.set_xlabel('Kelompok Usia')
            ax.set_ylabel('Jumlah Anggota')
            ax.set_title('Distribusi Usia Anggota Keluarga', fontsize=14, fontweight='bold')
            ax.set_xticks([(bins[i] + bins[i+1])/2 for i in range(len(bins)-1)])
            ax.set_xticklabels(labels, rotation=45)

            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            data = buf.getvalue()
            return {"age_hist": data, "age_distribution": data}

        except Exception as e:
            self._logger.warning(f"Age histogram failed: {e}")
            return {}

    def _create_gender_chart(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create gender distribution pie chart."""
        try:
            # Support multiple column names
            gender_col = None
            for col in ["gender_clean", "jenis_kelamin", "jenkel", "gender"]:
                if col in df.columns:
                    gender_col = col
                    break

            if not gender_col or df.empty:
                return {}

            counts = df[gender_col].value_counts()

            fig, ax = plt.subplots(figsize=(8, 8))
            colors = ['#1f77b4', '#ff69b4']

            ax.pie(counts.values, labels=counts.index, autopct='%1.1f%%',
                  colors=colors[:len(counts)], startangle=90)
            ax.set_title('Distribusi Jenis Kelamin', fontsize=14, fontweight='bold')

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            data = buf.getvalue()
            return {"gender_pie": data, "gender_distribution": data}

        except Exception as e:
            self._logger.warning(f"Gender chart failed: {e}")
            return {}

    def _create_population_pyramid(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create population pyramid (age & gender)."""
        try:
            if df.empty or "age" not in df.columns or "gender_clean" not in df.columns:
                return {}

            ages = pd.to_numeric(df["age"], errors='coerce')
            valid_members = df.loc[ages.notna()].copy()
            valid_members["age"] = ages[ages.notna()]
            valid_members["age_group"] = pd.cut(valid_members["age"], bins=range(0, 101, 5), right=False)

            if valid_members.empty:
                return {}

            gender_age = valid_members.groupby(["age_group", "gender_clean"]).size().unstack(fill_value=0)
            male_col = next((c for c in gender_age.columns if "laki" in str(c).lower() or "male" in str(c).lower() or str(c) == "1"), None)
            female_col = next((c for c in gender_age.columns if "perempuan" in str(c).lower() or "female" in str(c).lower() or str(c) == "2"), None)

            if male_col is None or female_col is None:
                return {}

            fig, ax = plt.subplots(figsize=(10, 8))
            y_pos = range(len(gender_age.index))

            ax.barh(y_pos, -gender_age[male_col], color='#3498db', label='Laki-laki')
            ax.barh(y_pos, gender_age[female_col], color='#e74c3c', label='Perempuan')

            ax.set_yticks(list(y_pos))
            ax.set_yticklabels([str(interval).replace("(", "").replace("]", "").replace(",", "-") for interval in gender_age.index])
            ax.set_xlabel('Populasi')
            ax.set_title('Piramida Penduduk (Usia & Gender)')
            ax.legend()
            ax.axvline(0, color='black', linewidth=0.8)
            ax.set_xticks(ax.get_xticks())
            ax.set_xticklabels([abs(int(x)) for x in ax.get_xticks()])

            plt.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"population_pyramid": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Population pyramid failed: {e}")
            return {}

    def _create_household_size(self, families: pd.DataFrame) -> Dict[str, bytes]:
        """Create household size distribution chart."""
        try:
            if families.empty:
                return {}

            size_col = next((c for c in ["jumlah_anggota_calc", "jumlah_anggota", "jml_anggota", "jumlah_art"] if c in families.columns), None)
            if not size_col:
                return {}

            sizes = pd.to_numeric(families[size_col], errors='coerce').dropna()
            if sizes.empty:
                return {}

            size_bins = [0, 1, 2, 3, 4, 5, 10, 999]
            size_labels = ["1", "2", "3", "4", "5", "6-10", "10+"]
            size_groups = pd.cut(sizes, bins=size_bins, labels=size_labels, right=False)
            size_counts = size_groups.value_counts().sort_index()

            fig, ax = plt.subplots(figsize=(8, 5))
            ax.bar(range(len(size_counts)), size_counts.values, color='#16a085')
            ax.set_xticks(range(len(size_counts)))
            ax.set_xticklabels(size_counts.index)
            ax.set_xlabel('Jumlah Anggota Keluarga')
            ax.set_ylabel('Jumlah Keluarga')
            ax.set_title('Distribusi Ukuran Rumah Tangga')
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return {"household_size": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Household size chart failed: {e}")
            return {}

    def _create_head_of_household_profile(self, members: pd.DataFrame) -> Dict[str, bytes]:
        """Create head-of-household gender profile chart."""
        try:
            if members.empty or "gender_clean" not in members.columns:
                return {}

            head_col = next((c for c in ["hub_kepala_keluarga", "hubungan_keluarga", "hubungan", "status_hubungan"] if c in members.columns), None)
            if not head_col:
                return {}

            heads = members[members[head_col].astype(str).str.contains("kepala", case=False, na=False)]
            if heads.empty:
                return {}

            gender_heads = heads["gender_clean"].value_counts()
            fig, ax = plt.subplots(figsize=(6, 6))
            colors_hoh = ['#3498db', '#e74c3c', '#95a5a6']
            ax.pie(gender_heads.values, labels=gender_heads.index, autopct='%1.1f%%', startangle=90, colors=colors_hoh[:len(gender_heads)])
            ax.set_title('Profil Kepala Rumah Tangga (Gender)')
            donut_circle = plt.Circle((0, 0), 0.70, fc='white')
            ax.add_artist(donut_circle)
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return {"head_of_household": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Head of household chart failed: {e}")
            return {}

    def _create_bansos_penetration(self, families: pd.DataFrame) -> Dict[str, bytes]:
        """Create bansos penetration stacked bar per desil."""
        try:
            if families.empty or "desil_class" not in families.columns or "bansos_combo" not in families.columns:
                return {}

            desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10"]
            families = families.copy()
            families["has_bansos"] = families["bansos_combo"].apply(lambda x: "Terima Bansos" if str(x) != "NO_BANSOS" else "Tidak Ada")
            crosstab = pd.crosstab(families["desil_class"], families["has_bansos"], normalize='index') * 100
            crosstab = crosstab.reindex(desil_order, fill_value=0)

            if crosstab.empty:
                return {}

            fig, ax = plt.subplots(figsize=(10, 6))
            crosstab.plot(kind='barh', stacked=True, ax=ax, color=['#27ae60', '#e74c3c'])
            ax.set_xlabel('Persentase (%)')
            ax.set_ylabel('Kelas Desil')
            ax.set_title('Penetrasi Bansos per Desil (Analisis Inklusi/Eksklusi)')
            ax.legend(title='Status')
            ax.set_xlim(0, 100)
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return {"bansos_penetration": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Bansos penetration chart failed: {e}")
            return {}

    def _create_desil_venn(self, files_dict: Optional[Dict[str, bytes]], families: pd.DataFrame) -> Dict[str, bytes]:
        """Create DESIL venn/overlap chart for low desil values."""
        try:
            families_raw = self._load_csv(files_dict.get("families_raw.csv")) if files_dict else pd.DataFrame()
            if families_raw.empty:
                return {}

            available_cols = [c for c in ["desil_nasional", "desil", "DESIL"] if c in families_raw.columns]
            desil_cols = available_cols[:3]
            if len(desil_cols) < 2:
                return {}

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

            if not any(len(s) for s in desil_sets):
                return {}

            fig_saved = False
            desil_png = io.BytesIO()
            try:
                if len(desil_sets) == 3:
                    fig, ax = plt.subplots(figsize=(6, 6))
                    venn3(desil_sets, tuple(labels[:3]), ax=ax)
                    ax.set_title("Irisan DESIL (Pendapatan Rendah)")
                    plt.tight_layout()
                    plt.savefig(desil_png, format='png', dpi=150, bbox_inches='tight')
                    plt.close(fig)
                    fig_saved = True
                elif len(desil_sets) == 2:
                    fig, ax = plt.subplots(figsize=(6, 6))
                    venn2(desil_sets[:2], tuple(labels[:2]), ax=ax)
                    ax.set_title("Irisan DESIL (Pendapatan Rendah)")
                    plt.tight_layout()
                    plt.savefig(desil_png, format='png', dpi=150, bbox_inches='tight')
                    plt.close(fig)
                    fig_saved = True
            except Exception:
                fig_saved = False

            if not fig_saved:
                counts = [len(s) for s in desil_sets]
                fig, ax = plt.subplots(figsize=(8, 5))
                ax.bar(labels, counts, color="#27ae60")
                ax.set_ylabel("Jumlah")
                ax.set_title("Jumlah Prioritas DESIL")
                plt.tight_layout()
                fallback_buf = io.BytesIO()
                plt.savefig(fallback_buf, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                fallback_buf.seek(0)
                return {"desil_venn": fallback_buf.getvalue()}

            desil_png.seek(0)
            return {"desil_venn": desil_png.getvalue()}
        except Exception as e:
            self._logger.warning(f"DESIL venn failed: {e}")
            return {}

    def _create_sanitation_water(self, aset_data: pd.DataFrame) -> Dict[str, bytes]:
        """Create sanitation and water access bars."""
        try:
            if aset_data is None or aset_data.empty:
                return {}

            water_col = next((c for c in ["sumber_air_minum", "air_minum"] if c in aset_data.columns), None)
            sanitation_col = next((c for c in ["fasilitas_bab", "kepemilikan_kamar_mandi", "pembuangan_tinja"] if c in aset_data.columns), None)
            if not water_col and not sanitation_col:
                return {}

            fig_cols = 2 if (water_col and sanitation_col) else 1
            fig, axes = plt.subplots(1, fig_cols, figsize=(12, 5))
            if not isinstance(axes, (list, np.ndarray)):
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

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return {"sanitation_water": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Sanitation & water chart failed: {e}")
            return {}

    def _create_age_by_desil(self, members: pd.DataFrame) -> Dict[str, bytes]:
        """Create median age per desil chart."""
        try:
            if members.empty or "desil_class" not in members.columns:
                return {}

            desil_order = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]
            age_desil = members.dropna(subset=["age", "desil_class"]).copy()
            age_desil["age"] = pd.to_numeric(age_desil["age"], errors='coerce')
            age_desil = age_desil.dropna(subset=["age"])
            if age_desil.empty:
                return {}

            medians = age_desil.groupby("desil_class")["age"].median().reindex(desil_order).dropna()
            if medians.empty:
                return {}

            fig, ax = plt.subplots(figsize=(9, 5))
            ax.bar(range(len(medians)), medians.values, color='#8e44ad')
            ax.set_xticks(range(len(medians)))
            ax.set_xticklabels(medians.index, rotation=30, ha='right')
            ax.set_ylabel('Median Usia')
            ax.set_title('Median Usia per DESIL')
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return {"age_by_desil": buf.getvalue()}
        except Exception as e:
            self._logger.warning(f"Age by desil chart failed: {e}")
            return {}

    def _create_housing_chart(self, df: pd.DataFrame, files_dict: Dict, aset_df: Optional[pd.DataFrame] = None) -> Dict[str, bytes]:
        """Create housing quality stacked bar chart."""
        try:
            aset = aset_df if isinstance(aset_df, pd.DataFrame) else self._load_csv(files_dict.get("aset_merged.csv") if files_dict else None)
            if aset.empty:
                return {}

            housing_cols = ["jenis_lantai", "jenis_dinding", "jenis_atap"]
            available_cols = [c for c in housing_cols if c in aset.columns]

            if not available_cols:
                return {}

            fig, axes = plt.subplots(1, len(available_cols), figsize=(15, 6))
            if len(available_cols) == 1:
                axes = [axes]

            for ax, col in zip(axes, available_cols):
                counts = aset[col].value_counts().head(10)
                ax.barh(range(len(counts)), counts.values, color='teal')
                ax.set_yticks(range(len(counts)))
                ax.set_yticklabels(counts.index)
                ax.set_xlabel('Jumlah')
                ax.set_title(col.replace("_", " ").title())

            plt.suptitle('Kualitas Perumahan', fontsize=14, fontweight='bold')
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"housing_quality": buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"Housing chart failed: {e}")
            return {}

    def _create_asset_heatmap(self, files_dict: Dict, aset_df: Optional[pd.DataFrame] = None) -> Dict[str, bytes]:
        """Create asset ownership heatmap."""
        try:
            aset = aset_df if isinstance(aset_df, pd.DataFrame) else self._load_csv(files_dict.get("aset_merged.csv") if files_dict else None)
            if aset.empty:
                return {}

            asset_cols = ["sepeda_motor", "smartphone", "televisi", "kulkas", "sepeda", "mobil"]
            available = [c for c in asset_cols if c in aset.columns]

            if len(available) < 2:
                return {}

            def _presence(val: Any) -> float:
                if isinstance(val, str):
                    s = val.strip().lower()
                    if s in {'ya', 'ada', 'y', 'true', '1'}:
                        return 1.0
                num = pd.to_numeric(val, errors='coerce')
                return float(num) if not pd.isna(num) else 0.0

            asset_numeric = aset[available].applymap(_presence)
            ownership_pct = (asset_numeric > 0).mean() * 100

            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.barh(range(len(ownership_pct)), ownership_pct.values, color='coral')
            ax.set_yticks(range(len(ownership_pct)))
            ax.set_yticklabels([c.replace("_", " ").title() for c in ownership_pct.index])
            ax.set_xlabel('Persentase Kepemilikan (%)')
            ax.set_title('Kepemilikan Aset Bergerak', fontsize=14, fontweight='bold')
            ax.set_xlim(0, 100)

            for bar, val in zip(bars, ownership_pct.values):
                ax.text(val + 1, bar.get_y() + bar.get_height()/2,
                       f'{val:.1f}%', va='center')

            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"asset_ownership_heatmap": buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"Asset heatmap failed: {e}")
            return {}

    def _create_poverty_hotspots(self, df: pd.DataFrame) -> Dict[str, bytes]:
        """Create poverty hotspots by RT/RW."""
        try:
            if df.empty:
                return {}

            rt_col = None
            rw_col = None
            for c in ["no_rt", "rt", "RT"]:
                if c in df.columns:
                    rt_col = c
                    break
            for c in ["no_rw", "rw", "RW"]:
                if c in df.columns:
                    rw_col = c
                    break

            if not rt_col or not rw_col:
                return {}

            # Group by RT/RW and count poor families
            df["rt_rw"] = df[rt_col].astype(str) + "/" + df[rw_col].astype(str)

            poor = df
            if "desil_class" in df.columns:
                poor = df[df["desil_class"].isin({"DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5"})]

            counts = poor["rt_rw"].value_counts().head(15)

            if counts.empty:
                return {}

            fig, ax = plt.subplots(figsize=(12, 8))
            bars = ax.barh(range(len(counts)), counts.values, color='crimson')
            ax.set_yticks(range(len(counts)))
            ax.set_yticklabels([f"RT/RW {x}" for x in counts.index])
            ax.set_xlabel('Jumlah Keluarga Miskin')
            ax.set_title('Hotspot Kemiskinan per RT/RW', fontsize=14, fontweight='bold')

            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {"poverty_hotspots": buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"Poverty hotspots chart failed: {e}")
            return {}

    def _fallback_bar(self, df: pd.DataFrame, cols: List[str],
                      title: str, key: str) -> Dict[str, bytes]:
        """Create simple bar chart as fallback."""
        try:
            counts = {c: df[c].sum() if c in df.columns else 0 for c in cols}

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(counts.keys(), counts.values(), color='steelblue')
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.set_ylabel('Jumlah')

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)

            return {key: buf.getvalue()}

        except Exception as e:
            self._logger.warning(f"Fallback bar chart failed: {e}")
            return {}

    def _load_csv(self, data: Optional[bytes]) -> pd.DataFrame:
        """Load CSV from bytes."""
        if not data:
            return pd.DataFrame()
        try:
            return pd.read_csv(io.BytesIO(data))
        except:
            return pd.DataFrame()

    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        try:
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except:
            pass
