"""
Report Generator - Premium Version
Creates Excel workbooks with enhanced formatting, financial calculations,
and proper text/date handling.

PREMIUM FEATURES:
- REKAP_PERINGKAT with total_pkh, total_bpnt, grand_total calculations
- Text format for no_kk to preserve leading zeros
- Currency formatting for financial columns
- Extended column filtering
- Proper date handling
- Frozen panes and auto-fit columns
"""

import io
import zipfile
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd
import xlsxwriter

from core import get_logger, cleanup_resources, MemoryMonitor


class ReportGenerator:
    """Creates premium Excel workbooks with enhanced formatting and calculations."""

    # Columns to drop from output
    COLS_TO_DROP = {
        'id_keluarga', 'id_keluarga_parent', 'idsemesta', 'ID_KELUARGA', 'IDSEMESTA',
        'id_keluarga_aset', 'id_keluarga_parent_pbi',
        'id_keluarga_kyc', 'no_kk_kyc', 'nik_input', 'id_jenis_kelamin',
        'id_status_perkawinan', 'id_hub_keluarga', 'id_keluarga_pbi',
        'id_deleted', 'alasan_tolak_meninggal', 'nama_input',
        'no_prop', 'no_kab', 'no_kec', 'no_kel',
        'id_pekerjaan_utama', 'id_keluarga_parent_kyc', 'idsemesta_pbi',
        # Intermediate flags (not in legacy output)
        'pkh_flag', 'bpnt_flag', 'pbi_flag'
    }

    # Columns that should be formatted as text
    TEXT_COLS = {'nik', 'no_kk', 'NIK', 'NO_KK', 'nik_anggota', 'nomor_kartu', 'nomor_kks', 'nomor_pkh'}

    # Columns that should be formatted as dates
    DATE_COLS = {'tgl_lahir', 'tanggal_lahir', 'TGL_LAHIR', 'tanggal_pencairan', 'tanggal_pembayaran', 'tanggal'}

    # Column ordering constants strictly from aaa.txt / legacy
    ORDER_KELUARGA = [
        "no_kk", "nama_kepala_keluarga", "jumlah_anggota_calc", "alamat",
        "no_rt", "no_rw", "desil_nasional", "peringkat_nasional",
        "bansos_combo", "desil_class"
    ]

    ORDER_ANGGOTA = [
        "nama", "nik", "tgl_lahir", "gender_clean", "hubungan_keluarga",
        "status_kawin", "age", "bansos_combo", "desil_class"
    ]

    ORDER_REKAP = [
        "no_kk", "nama_kepala_keluarga", "alamat", "no_rt", "no_rw",
        "desil_nasional", "peringkat_nasional", "bansos_combo",
        "total_pkh", "total_bpnt", "grand_total"
    ]

    def __init__(self, data_processor=None, visualizer=None):
        self._logger = get_logger('report_generator')
        self.data_processor = data_processor
        self.visualizer = visualizer
        self._memory = MemoryMonitor(warning_threshold_mb=300)

    @cleanup_resources
    def build_xlsx(self, files_dict: Dict[str, bytes],
                   keluarga_master: pd.DataFrame,
                   anggota_master: pd.DataFrame,
                   desil_sheets: Dict[str, pd.DataFrame],
                   charts: Dict[str, bytes] = None) -> bytes:
        """Build enhanced XLSX with master sheets, desil breakdown, and visualizations."""
        self._memory.log_usage("build_xlsx_start")
        self._logger.info("Building XLSX report")

        output = io.BytesIO()

        # Load additional data
        pkh = self._load_csv(files_dict.get("pkh_raw.csv"))
        bpnt = self._load_csv(files_dict.get("bpnt_raw.csv"))
        pbi = self._load_csv(files_dict.get("pbi_raw.csv"))
        aset = self._load_csv(files_dict.get("aset_merged.csv"))

        # Create summaries
        summary_bansos = self._create_bansos_summary(keluarga_master)
        summary_desil = self._create_desil_summary(keluarga_master)

        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            workbook = writer.book

            # Format definitions
            header_fmt = workbook.add_format({
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#2F75B5",
                "align": "center",
                "border": 1
            })
            cell_fmt = workbook.add_format({"border": 1, "valign": "vcenter"})
            odd_fmt = workbook.add_format({"bg_color": "#F7F7F7"})
            even_fmt = workbook.add_format({"bg_color": "#FFFFFF"})
            text_fmt = workbook.add_format({'num_format': '@'})
            date_fmt = workbook.add_format({'num_format': 'dd/mm/yyyy'})
            currency_fmt = workbook.add_format({'num_format': '#,##0', 'align': 'right'})

            # Write master sheets
            if not keluarga_master.empty:
                df_km = self._prepare_df_for_excel(keluarga_master)
                self._write_sheet(writer, workbook, "KELUARGA_MASTER", df_km, header_fmt, cell_fmt, text_fmt)

            if not anggota_master.empty:
                df_am = self._prepare_df_for_excel(anggota_master)
                self._write_sheet(writer, workbook, "ANGGOTA_MASTER", df_am, header_fmt, cell_fmt, text_fmt)

            # Write desil sheets
            for sheet_name, df in desil_sheets.items():
                df_d = self._prepare_df_for_excel(df)
                self._write_sheet(writer, workbook, sheet_name[:31], df_d, header_fmt, cell_fmt, text_fmt)

            # Write detail sheets (renamed from RAW to DETAIL)
            if not pkh.empty:
                df_pkh = self._prepare_df_for_excel(pkh)
                self._write_sheet(writer, workbook, "PKH_DETAIL", df_pkh, header_fmt, cell_fmt, text_fmt)

            if not bpnt.empty:
                df_bpnt = self._prepare_df_for_excel(bpnt)
                self._write_sheet(writer, workbook, "BPNT_DETAIL", df_bpnt, header_fmt, cell_fmt, text_fmt)

            if not pbi.empty:
                df_pbi = self._prepare_df_for_excel(pbi)
                self._write_sheet(writer, workbook, "PBI_DETAIL", df_pbi, header_fmt, cell_fmt, text_fmt)

            if not aset.empty:
                df_aset = self._prepare_df_for_excel(aset)
                self._write_sheet(writer, workbook, "ASET_DETAIL", df_aset, header_fmt, cell_fmt, text_fmt)

            # --- REKAP_PERINGKAT Sheet with Financial Calculations ---
            if not keluarga_master.empty:
                self._write_rekap_peringkat(writer, workbook, keluarga_master, anggota_master,
                                            pkh, bpnt, header_fmt, cell_fmt, text_fmt, currency_fmt)

            # Write summary sheets (renamed)
            summary_bansos.to_excel(writer, sheet_name="SUMMARY_BY_BANSOS", index=False)
            summary_desil.to_excel(writer, sheet_name="SUMMARY_BY_DESIL", index=False)

            # Apply formatting to all sheets
            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 0)  # Freeze top row

            # Visualizations sheet
            if charts:
                self._write_visualizations(workbook, charts)

        self._memory.log_usage("build_xlsx_end")
        self._logger.info("XLSX generation complete")
        return output.getvalue()

    def build_xlsx_with_fallback(self, files_dict: Dict[str, bytes],
                                  keluarga_master: pd.DataFrame,
                                  anggota_master: pd.DataFrame,
                                  desil_sheets: Dict[str, pd.DataFrame],
                                  charts: Dict[str, bytes] = None) -> Tuple[bytes, str]:
        """Build XLSX with automatic fallback to CSV on failure.

        Returns:
            Tuple of (bytes, format) where format is 'xlsx' or 'csv_zip'
        """
        try:
            xlsx_bytes = self.build_xlsx(files_dict, keluarga_master, anggota_master, desil_sheets, charts)
            return (xlsx_bytes, 'xlsx')
        except Exception as e:
            self._logger.error(f"XLSX generation failed: {e}, falling back to CSV")
            return self._build_fallback_csv(files_dict, keluarga_master, anggota_master)

    def _build_fallback_csv(self, files_dict: Dict[str, bytes],
                            keluarga_master: pd.DataFrame,
                            anggota_master: pd.DataFrame) -> Tuple[bytes, str]:
        """Generate CSV files as fallback when Excel fails."""
        self._logger.info("Generating fallback CSV files")

        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
            if not keluarga_master.empty:
                zf.writestr("KELUARGA_MASTER.csv", keluarga_master.to_csv(index=False))
            if not anggota_master.empty:
                zf.writestr("ANGGOTA_MASTER.csv", anggota_master.to_csv(index=False))

            # Add raw data files
            for name, data in files_dict.items():
                if name.endswith('.csv'):
                    zf.writestr(name, data)

        self._logger.info("Fallback CSV generation complete")
        return (output.getvalue(), 'csv_zip')

    # Helper methods

    def _load_csv(self, data: Optional[bytes]) -> pd.DataFrame:
        """Load CSV from bytes."""
        if not data:
            return pd.DataFrame()
        try:
            return pd.read_csv(io.BytesIO(data))
        except:
            return pd.DataFrame()

    def _prepare_df_for_excel(self, df_in: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for Excel export with proper formatting."""
        if df_in.empty:
            return df_in

        d = df_in.copy()

        # Drop unwanted ID columns
        drop_these = [c for c in d.columns if c in self.COLS_TO_DROP]
        d.drop(columns=drop_these, inplace=True, errors='ignore')

        # Convert text columns to string to preserve leading zeros
        for c in d.columns:
            if c in self.TEXT_COLS:
                d[c] = d[c].astype(str)

        # Convert date columns
        for c in d.columns:
            if c in self.DATE_COLS or 'tanggal' in c.lower() or 'tgl' in c.lower():
                d[c] = pd.to_datetime(d[c], errors='coerce').dt.date

        return d

    def _write_sheet(self, writer, workbook, name: str, df: pd.DataFrame,
                     header_fmt, cell_fmt, text_fmt):
        """Write DataFrame to worksheet with formatting."""
        if df.empty:
            return

        # Reorder columns based on sheet name
        if name == "KELUARGA_MASTER" or name.startswith("DESIL"):
            # Start with defined order
            cols = [c for c in self.ORDER_KELUARGA if c in df.columns]
            # Append remaining columns (e.g. assets) that are not in explicit list
            existing = set(cols)
            cols.extend([c for c in df.columns if c not in existing])
            df = df[cols]
        elif name == "ANGGOTA_MASTER":
            cols = [c for c in self.ORDER_ANGGOTA if c in df.columns]
            existing = set(cols)
            cols.extend([c for c in df.columns if c not in existing])
            df = df[cols]

        # Write to Excel
        sheet_name = name[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        ws = writer.sheets[sheet_name]

        # Apply header formatting
        for col_idx, col_name in enumerate(df.columns):
            ws.write(0, col_idx, str(col_name), header_fmt)

        # Apply text format to NIK/KK columns
        for col_idx, col_name in enumerate(df.columns):
            if col_name in self.TEXT_COLS:
                ws.set_column(col_idx, col_idx, None, text_fmt)

        # Autofit columns
        self._autofit_columns(ws, df)

        # Apply row striping (alternating colors)
        fmt_odd = workbook.add_format({'bg_color': '#F7F7F7'})
        # Range excludes header (row 0), starts at row 1
        # Excel rows are 1-based in conditional_format strings usually,
        # but xlsxwriter takes (row, col, last_row, last_col) 0-indexed.
        last_row = len(df)
        last_col = len(df.columns) - 1
        if last_row > 0:
            ws.conditional_format(1, 0, last_row, last_col, {
                'type': 'formula',
                'criteria': '=MOD(ROW(),2)=0',
                'format': fmt_odd
            })

    def _autofit_columns(self, ws, df: pd.DataFrame):
        """Auto-fit column widths."""
        for idx, col in enumerate(df.columns):
            max_len = len(str(col))
            for val in df[col].head(100):
                if not pd.isna(val):
                    max_len = max(max_len, len(str(val)))
            ws.set_column(idx, idx, min(max_len + 2, 50))

    def _create_bansos_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create bansos combination summary."""
        if df.empty or "bansos_combo" not in df.columns:
            return pd.DataFrame(columns=["bansos_combo", "jumlah_keluarga"])
        return df.groupby("bansos_combo").size().reset_index(
            name="jumlah_keluarga"
        ).sort_values("jumlah_keluarga", ascending=False)

    def _create_desil_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create desil class summary."""
        if df.empty or "desil_class" not in df.columns:
            return pd.DataFrame(columns=["desil_class", "jumlah_keluarga"])
        return df.groupby("desil_class").size().reset_index(
            name="jumlah_keluarga"
        ).sort_values("desil_class")

    def _write_rekap_peringkat(self, writer, workbook, keluarga_master: pd.DataFrame,
                                anggota_master: pd.DataFrame, pkh: pd.DataFrame,
                                bpnt: pd.DataFrame, header_fmt, cell_fmt, text_fmt, currency_fmt):
        """Write REKAP_PERINGKAT sheet with financial calculations."""
        self._logger.info("Building REKAP_PERINGKAT...")

        rekap_df = keluarga_master.copy()

        # Ensure id_keluarga exists for merging (restore if dropped)
        if "id_keluarga" not in rekap_df.columns:
            for alt in ["id_keluarga_parent", "ID_KELUARGA"]:
                if alt in rekap_df.columns:
                    rekap_df["id_keluarga"] = rekap_df[alt]
                    break
            else:
                self._logger.warning("No id_keluarga column found for REKAP_PERINGKAT merge")
                return

        # Ensure ID is string for merging
        rekap_df["id_keluarga"] = rekap_df["id_keluarga"].astype(str)

        # Helper to clean nominal values
        def clean_nominal(val):
            if pd.isna(val):
                return 0
            s = str(val).replace("Rp", "").replace(".", "").replace(",", "").strip()
            try:
                return int(s)
            except:
                return 0

        # Calculate PKH Total Nominal
        pkh_totals = pd.DataFrame(columns=["id_keluarga", "total_pkh"])
        if not pkh.empty and "id_keluarga" in pkh.columns:
            pkh_temp = pkh.copy()
            pkh_temp["id_keluarga"] = pkh_temp["id_keluarga"].astype(str)

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

        # Ranking Logic - sort by peringkat_nasional
        if "peringkat_nasional" in rekap_df.columns:
            rekap_df["rank_sort"] = pd.to_numeric(rekap_df["peringkat_nasional"], errors='coerce')
            rekap_df = rekap_df.sort_values("rank_sort", ascending=True, na_position='last')
            rekap_df.drop(columns=["rank_sort"], inplace=True)

        # Select columns - STRICT ORDER for REKAP_PERINGKAT
        cols_to_keep = self.ORDER_REKAP

        # Filter existing columns only
        final_cols = [c for c in cols_to_keep if c in rekap_df.columns]
        rekap_final = rekap_df[final_cols].copy()

        # Force no_kk to string to avoid scientific notation
        if "no_kk" in rekap_final.columns:
            rekap_final["no_kk"] = rekap_final["no_kk"].astype(str)

        # Write to Excel
        rekap_final.to_excel(writer, sheet_name="REKAP_PERINGKAT", index=False)

        # Get worksheet and apply formatting
        ws_rekap = writer.sheets["REKAP_PERINGKAT"]
        ws_rekap.freeze_panes(1, 0)  # Freeze top row

        # Add AutoFilter
        if len(rekap_final) > 0:
            ws_rekap.autofilter(0, 0, len(rekap_final), len(final_cols) - 1)

        # Apply header formatting
        for col_idx, col_name in enumerate(final_cols):
            ws_rekap.write(0, col_idx, col_name, header_fmt)

        # Get column indices
        pkh_idx = final_cols.index("total_pkh") if "total_pkh" in final_cols else -1
        bpnt_idx = final_cols.index("total_bpnt") if "total_bpnt" in final_cols else -1
        grand_idx = final_cols.index("grand_total") if "grand_total" in final_cols else -1
        nokk_idx = final_cols.index("no_kk") if "no_kk" in final_cols else -1
        rt_idx = final_cols.index("no_rt") if "no_rt" in final_cols else -1
        rw_idx = final_cols.index("no_rw") if "no_rw" in final_cols else -1

        # Apply formats and widths
        if pkh_idx >= 0: ws_rekap.set_column(pkh_idx, pkh_idx, 15, currency_fmt)
        if bpnt_idx >= 0: ws_rekap.set_column(bpnt_idx, bpnt_idx, 15, currency_fmt)
        if grand_idx >= 0: ws_rekap.set_column(grand_idx, grand_idx, 18, currency_fmt)
        if nokk_idx >= 0: ws_rekap.set_column(nokk_idx, nokk_idx, 20, text_fmt)
        if rt_idx >= 0: ws_rekap.set_column(rt_idx, rt_idx, 8, text_fmt)
        if rw_idx >= 0: ws_rekap.set_column(rw_idx, rw_idx, 8, text_fmt)

        # Autofit others
        for i, col in enumerate(final_cols):
            if i not in [pkh_idx, bpnt_idx, grand_idx, nokk_idx, rt_idx, rw_idx]:
                max_len = len(str(col)) + 2
                ws_rekap.set_column(i, i, min(max_len + 5, 40))

        # Apply row striping (alternating colors)
        fmt_odd = workbook.add_format({'bg_color': '#F7F7F7'})
        last_row = len(rekap_final)
        last_col = len(rekap_final.columns) - 1
        if last_row > 0:
            ws_rekap.conditional_format(1, 0, last_row, last_col, {
                'type': 'formula',
                'criteria': '=MOD(ROW(),2)=0',
                'format': fmt_odd
            })

    def _write_visualizations(self, wb, charts: Dict[str, bytes]):
        """Write visualizations sheet with embedded images."""
        vis_ws = wb.add_worksheet("VISUALIZATIONS")
        row = 0

        for key, img_bytes in charts.items():
            try:
                vis_ws.write(row, 0, key.upper().replace("_", " "))
                row += 1

                # Write image
                img_io = io.BytesIO(img_bytes)
                vis_ws.insert_image(row, 0, key, {'image_data': img_io, 'x_scale': 0.8, 'y_scale': 0.8})
                row += 25

            except Exception as e:
                vis_ws.write(row, 0, f"Failed to insert {key}: {e}")
                row += 2
