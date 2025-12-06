"""
PDF Builder - Premium Version
Handles PDF report generation with 7-section family details, premium styling,
pagination, accessibility, and branding.

PREMIUM DESIGN FEATURES:
- F4 page size (21.5 x 33 cm)
- Professional slate/blue color scheme (#2980B9, #2C3E50)
- Logo integration in header (assets/icon_p.png)
- Enhanced footer with page number, timestamp, and CONFIDENTIAL watermark
- Alternating row colors (#F4F6F7) for better readability
- Formatted currency values (Rp. 1.500.000)
- 7 sections: Family, Members, Aset Tidak Bergerak, Aset Bergerak, PBI, BPNT, PKH
"""

import io
import os
import html
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

import pandas as pd

from core import get_logger, cleanup_resources, MemoryMonitor
from .constants import PAGE_SIZE_F4, FAMILY_HEADERS, MEMBER_HEADERS, ASSET_IMMOVABLE, ASSET_MOVABLE

# PDF imports
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.units import cm, inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image, PageBreak, KeepTogether, PageTemplate, Frame
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.pdfencrypt import StandardEncryption
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


class PDFBuilder:
    """Builds premium PDF reports with 7-section family details, proper formatting, accessibility, and branding."""

    def __init__(self, data_processor=None, visualizer=None,
                 logo_path: str = None, organization: str = "Kementerian Sosial"):
        self._logger = get_logger('pdf_builder')
        self.data_processor = data_processor
        self.visualizer = visualizer
        self.logo_path = logo_path or os.path.join("assets", "icon_p.png")
        self.organization = organization
        self._memory = MemoryMonitor(warning_threshold_mb=300)

        # Styles
        self.styles = None
        self._init_styles()

    def _init_styles(self):
        """Initialize premium PDF styles with professional color scheme."""
        if not REPORTLAB_AVAILABLE:
            return

        self.styles = getSampleStyleSheet()

        # Premium Custom Styles with slate/blue theme
        self.styles.add(ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Title'],
            fontSize=22,
            textColor=colors.HexColor("#2C3E50"),
            spaceAfter=20,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))

        self.styles.add(ParagraphStyle(
            'CustomH2',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=colors.white,
            backColor=colors.HexColor("#2C3E50"),
            borderPadding=8,
            spaceBefore=15,
            spaceAfter=10,
            keepWithNext=True,
            fontName='Helvetica-Bold'
        ))

        self.styles.add(ParagraphStyle(
            'CustomH3',
            parent=self.styles['Heading3'],
            fontSize=12,
            textColor=colors.HexColor("#2980B9"),
            spaceBefore=10,
            spaceAfter=6,
            keepWithNext=True,
            fontName='Helvetica-Bold'
        ))

        self.styles.add(ParagraphStyle(
            'CustomNormal',
            parent=self.styles['BodyText'],
            fontSize=10,
            leading=12
        ))

        self.styles.add(ParagraphStyle(
            'SmallBody',
            parent=self.styles['BodyText'],
            fontSize=8,
            leading=10,
            alignment=TA_CENTER
        ))

        self.styles.add(ParagraphStyle(
            'SmallBodyLeft',
            parent=self.styles['BodyText'],
            fontSize=8,
            leading=10,
            alignment=TA_LEFT
        ))

    @cleanup_resources
    def build_pdfs(self, files_dict: Dict[str, bytes],
                   keluarga_master: pd.DataFrame,
                   anggota_master: pd.DataFrame,
                   charts: Dict[str, bytes] = None) -> Dict[str, bytes]:
        """Build multiple PDF reports split by Desil groups."""
        if not REPORTLAB_AVAILABLE:
            self._logger.error("ReportLab not available - cannot generate PDFs")
            return {}

        self._memory.log_usage("build_pdfs_start")
        self._logger.info("Building PDF reports")

        if keluarga_master.empty:
            self._logger.warning("No family data - skipping PDF generation")
            return {}

        # Ensure desil_class exists
        if "desil_class" not in keluarga_master.columns:
            if "desil_nasional" in keluarga_master.columns:
                keluarga_master["desil_class"] = keluarga_master["desil_nasional"].apply(self._map_desil)
            elif "desil" in keluarga_master.columns:
                keluarga_master["desil_class"] = keluarga_master["desil"].apply(self._map_desil)
            else:
                keluarga_master["desil_class"] = "DESIL_BELUM_DITENTUKAN"

        # Group by desil
        groups = list(keluarga_master.groupby("desil_class"))

        pdf_files = {}

        # 1. Generate FULL REPORT
        self._logger.info("Generating FULL REPORT (All Families)...")
        try:
            pdf_full = self._generate_single_pdf(
                "FULL REPORT", keluarga_master, anggota_master, files_dict, charts
            )
            pdf_files["FULL_REPORT.pdf"] = pdf_full
        except Exception as e:
            self._logger.error(f"Error generating FULL REPORT: {e}")

        # 2. Generate Per-Desil Reports
        for desil_name, group_df in groups:
            if group_df.empty:
                continue

            self._logger.info(f"Generating report for {desil_name} ({len(group_df)} families)...")

            try:
                # Filter members
                valid_ids = set(group_df["id_keluarga"].astype(str))
                group_members = pd.DataFrame()
                if not anggota_master.empty:
                    if "id_keluarga" in anggota_master.columns:
                        mask = anggota_master["id_keluarga"].astype(str).isin(valid_ids)
                        group_members = anggota_master[mask].copy()
                    elif "id_keluarga_parent" in anggota_master.columns:
                        mask = anggota_master["id_keluarga_parent"].astype(str).isin(valid_ids)
                        group_members = anggota_master[mask].copy()

                pdf_bytes = self._generate_single_pdf(
                    desil_name, group_df, group_members, files_dict, charts
                )

                safe_name = str(desil_name).replace(" ", "_").upper()
                pdf_files[f"{safe_name}_REPORT.pdf"] = pdf_bytes

            except Exception as e:
                self._logger.error(f"Error generating report for {desil_name}: {e}")

        self._memory.log_usage("build_pdfs_end")
        self._logger.info(f"Generated {len(pdf_files)} PDF files")
        return pdf_files

    def _generate_single_pdf(self, title_suffix: str, families: pd.DataFrame,
                             members: pd.DataFrame, files_dict: Dict[str, bytes],
                             charts: Dict[str, bytes] = None) -> bytes:
        """
        Generate a premium PDF report for families with 7 data sections.
        """
        output = io.BytesIO()
        PAGE_SIZE = PAGE_SIZE_F4

        # Store for header/footer callback
        self._current_page_size = PAGE_SIZE

        doc = SimpleDocTemplate(
            output,
            pagesize=PAGE_SIZE,
            leftMargin=1.5 * cm,
            rightMargin=1.5 * cm,
            topMargin=2.5 * cm,
            bottomMargin=2.0 * cm,
            title=f"Laporan DTSEN - {title_suffix}",
            author=self.organization,
            subject="Laporan Data Sosial Ekonomi",
            creator="DTSEN Scraper Pro"
        )

        story = []
        page_width = PAGE_SIZE[0] - doc.leftMargin - doc.rightMargin

        # Load bansos details
        pkh = self._load_csv(files_dict.get("pkh_raw.csv"))
        bpnt = self._load_csv(files_dict.get("bpnt_raw.csv"))
        pbi = self._load_csv(files_dict.get("pbi_raw.csv"))
        aset_merged = self._load_csv(files_dict.get("aset_merged.csv"))

        # --- COVER PAGE ---
        story.append(Paragraph(f"DTSEN Report - {title_suffix}", self.styles['CustomTitle']))
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

        # Add visualizations section if charts available
        if charts:
            story.extend(self._create_visualization_dashboard(charts, page_width))

        story.append(PageBreak())

        # --- FAMILY DETAILS with 7 SECTIONS ---
        family_records = families.to_dict(orient="records")

        for idx, family_row in enumerate(family_records, 1):
            try:
                id_kel = str(family_row.get("id_keluarga") or family_row.get("ID_KELUARGA") or "").strip()
                if id_kel.lower() in {"nan", "none", ""}:
                    continue

                # Filter members for this family
                fam_members = pd.DataFrame()
                if not members.empty:
                    if "id_keluarga" in members.columns:
                        fam_members = members[members["id_keluarga"].astype(str) == id_kel]
                    elif "id_keluarga_parent" in members.columns:
                        fam_members = members[members["id_keluarga_parent"].astype(str) == id_kel]

                # Calculate member count
                family_row["jumlah_anggota_calc"] = str(len(fam_members))

                # Extract RT/RW from members if available
                if not fam_members.empty:
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
                head_text = f"Family #{idx}: {html.escape(self._safe_text(family_row.get('nama_kepala_keluarga','-')))}"
                story.append(Paragraph(f"<b>{head_text}</b>", self.styles['CustomH2']))

                f_table_data = []
                for label, keys in FAMILY_HEADERS:
                    val = self._pick_value(family_row, keys)
                    f_table_data.append([
                        Paragraph(f"<b>{label}</b>", self.styles['CustomNormal']),
                        Paragraph(val, self.styles['CustomNormal'])
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
                story.append(Paragraph("<b>2. Anggota Keluarga</b>", self.styles['CustomH3']))

                if not fam_members.empty:
                    m_header_row = [Paragraph(f"<b>{h[0]}</b>", self.styles['SmallBody']) for h in MEMBER_HEADERS]
                    m_data = [m_header_row]

                    for _, mem_row in fam_members.iterrows():
                        row_cells = []
                        for label, keys in MEMBER_HEADERS:
                            val = self._pick_value(mem_row, keys)
                            if label == "Tgl Lahir":
                                val = self._fmt_date(val)
                            row_cells.append(Paragraph(val, self.styles['SmallBodyLeft']))
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
                    story.append(Paragraph("No members found.", self.styles['SmallBody']))

                story.append(Spacer(1, 0.15 * inch))

                # Get asset data for this family
                asset_row = None
                if not aset_merged.empty:
                    if "id_keluarga" in aset_merged.columns:
                        matches = aset_merged[aset_merged["id_keluarga"].astype(str) == id_kel]
                        if not matches.empty:
                            asset_row = matches.iloc[0]

                # === SECTION 3: ASET TIDAK BERGERAK (Immovable Assets) ===
                story.append(Paragraph("<b>3. Aset Tidak Bergerak</b>", self.styles['CustomH3']))

                a1_data = [[Paragraph("<b>Asset Item</b>", self.styles['CustomNormal']),
                           Paragraph("<b>Value</b>", self.styles['CustomNormal'])]]

                if asset_row is not None:
                    for label, keys in ASSET_IMMOVABLE:
                        val = self._pick_value(asset_row, keys)
                        a1_data.append([Paragraph(label, self.styles['CustomNormal']),
                                       Paragraph(val, self.styles['CustomNormal'])])
                else:
                    a1_data.append([Paragraph("No data", self.styles['CustomNormal']),
                                   Paragraph("-", self.styles['CustomNormal'])])

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
                story.append(Paragraph("<b>4. Aset Bergerak</b>", self.styles['CustomH3']))

                header_row = [
                    Paragraph("<b>Asset Item</b>", self.styles['CustomNormal']),
                    Paragraph("<b>Value</b>", self.styles['CustomNormal']),
                    Paragraph("<b>Asset Item</b>", self.styles['CustomNormal']),
                    Paragraph("<b>Value</b>", self.styles['CustomNormal']),
                ]
                a2_data = [header_row]

                movable_pairs = []
                if asset_row is not None:
                    for label, keys in ASSET_MOVABLE:
                        val = self._pick_value(asset_row, keys)
                        movable_pairs.append((
                            Paragraph(label, self.styles['CustomNormal']),
                            Paragraph(val, self.styles['CustomNormal'])
                        ))
                else:
                    movable_pairs.append((
                        Paragraph("No data", self.styles['CustomNormal']),
                        Paragraph("-", self.styles['CustomNormal'])
                    ))

                for i in range(0, len(movable_pairs), 2):
                    first_label, first_value = movable_pairs[i]
                    if i + 1 < len(movable_pairs):
                        second_label, second_value = movable_pairs[i + 1]
                    else:
                        second_label = Paragraph("", self.styles['CustomNormal'])
                        second_value = Paragraph("", self.styles['CustomNormal'])
                    a2_data.append([first_label, first_value, second_label, second_value])

                t_assets2 = Table(a2_data, colWidths=[
                    page_width * 0.3, page_width * 0.2,
                    page_width * 0.3, page_width * 0.2,
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

                # For PBI, check both id_keluarga_parent and id_keluarga
                pbi_rows = pd.DataFrame()
                if not pbi.empty:
                    if "id_keluarga_parent" in pbi.columns:
                        pbi_rows = pbi[pbi["id_keluarga_parent"].astype(str) == id_kel]
                    elif "id_keluarga" in pbi.columns:
                        pbi_rows = pbi[pbi["id_keluarga"].astype(str) == id_kel]

                # === SECTION 5: PBI INFO ===
                if not pbi_rows.empty:
                    story.append(Paragraph("<b>5. PBI Information</b>", self.styles['CustomH3']))
                    pbi_headers = ["NAMA", "NIK", "PERIODE AWAL", "PERIODE AKHIR"]
                    pbi_data = [[Paragraph(f"<b>{h}</b>", self.styles['SmallBody']) for h in pbi_headers]]

                    for _, prow in pbi_rows.iterrows():
                        p_nama = self._pick_value(prow, ["nama", "nama_lengkap", "NAMA", "nama_peserta"])
                        p_nik = self._pick_value(prow, ["nik", "NIK", "nik_peserta"])
                        p_awal = self._pick_value(prow, ["periode_awal", "nama_periode"])
                        p_akhir = self._pick_value(prow, ["periode_akhir"])
                        pbi_data.append([
                            Paragraph(p_nama, self.styles['SmallBodyLeft']),
                            Paragraph(p_nik, self.styles['SmallBodyLeft']),
                            Paragraph(p_awal, self.styles['SmallBody']),
                            Paragraph(p_akhir, self.styles['SmallBody'])
                        ])

                    t_pbi = Table(pbi_data, colWidths=[page_width*0.3, page_width*0.25, page_width*0.225, page_width*0.225])
                    t_pbi.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#27AE60")),  # Green
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
                    story.append(Paragraph("<b>6. BPNT Information</b>", self.styles['CustomH3']))
                    bpnt_headers = ["TAHAP/PERIODE", "STATUS", "NOMINAL"]
                    bpnt_data = [[Paragraph(f"<b>{h}</b>", self.styles['SmallBody']) for h in bpnt_headers]]

                    for _, brow in bpnt_rows.iterrows():
                        b_tahap = self._pick_value(brow, ["tahap", "periode", "bulan", "nama_periode"])
                        b_status = self._pick_value(brow, ["status", "keterangan_transaksi", "status_transaksi"])
                        b_nom = self._fmt_rupiah(self._pick_value(brow, ["nominal", "jumlah_bantuan", "nominal_bansos"]))
                        bpnt_data.append([
                            Paragraph(b_tahap, self.styles['SmallBody']),
                            Paragraph(b_status, self.styles['SmallBody']),
                            Paragraph(b_nom, self.styles['SmallBody'])
                        ])

                    t_bpnt = Table(bpnt_data, colWidths=[page_width*0.4, page_width*0.35, page_width*0.25])
                    t_bpnt.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F39C12")),  # Orange
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
                    story.append(Paragraph("<b>7. PKH Information</b>", self.styles['CustomH3']))
                    pkh_headers = ["TAHAP", "STATUS", "KOMPONEN", "NOMINAL"]
                    pkh_data = [[Paragraph(f"<b>{h}</b>", self.styles['SmallBody']) for h in pkh_headers]]

                    for _, krow in pkh_rows.iterrows():
                        k_tahap = self._pick_value(krow, ["tahap", "periode"])
                        k_status = self._pick_value(krow, ["status", "status_transaksi"])
                        k_komp = self._pick_value(krow, ["komponen", "kategori", "jenis_bantuan"])
                        k_nom = self._fmt_rupiah(self._pick_value(krow, ["nominal", "jumlah_bantuan", "nominal_bansos"]))
                        pkh_data.append([
                            Paragraph(k_tahap, self.styles['SmallBody']),
                            Paragraph(k_status, self.styles['SmallBody']),
                            Paragraph(k_komp, self.styles['SmallBody']),
                            Paragraph(k_nom, self.styles['SmallBody'])
                        ])

                    t_pkh = Table(pkh_data, colWidths=[page_width*0.2, page_width*0.3, page_width*0.3, page_width*0.2])
                    t_pkh.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#8E44AD")),  # Purple
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
                self._logger.warning(f"Error creating PDF page for family {idx}: {e}")
                continue

        # Build PDF with header/footer
        doc.build(story, onFirstPage=self._add_header_footer, onLaterPages=self._add_header_footer)
        return output.getvalue()

    def _create_visualization_dashboard(self, charts: Dict[str, bytes], page_width: float) -> List:
        """Create comprehensive visualization dashboard with all available charts."""
        elements = []

        elements.append(Paragraph("<b>RINGKASAN VISUALISASI DATA</b>", self.styles['CustomH2']))
        elements.append(Spacer(1, 0.2 * inch))

        # Section 1: Demographics & Population Structure
        demo_charts = ["population_pyramid", "household_size", "head_of_household", "gender_pie"]
        demo_available = [k for k in demo_charts if k in charts]

        if demo_available:
            elements.append(Paragraph("<b>1. Demographics & Population Structure</b>", self.styles['CustomH3']))
            elements.append(Spacer(1, 0.1 * inch))

            for i in range(0, len(demo_available), 2):
                row = []
                for j in range(2):
                    if i + j < len(demo_available):
                        key = demo_available[i + j]
                        try:
                            img_io = io.BytesIO(charts[key])
                            row.append(Image(img_io, width=3.2*inch, height=2.8*inch))
                        except:
                            pass
                if row:
                    t = Table([row], colWidths=[page_width/2]*len(row))
                    t.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                    elements.append(t)
                    elements.append(Spacer(1, 0.2 * inch))

        # Section 2: Welfare & Social Aid Analysis
        welfare_charts = ["desil_distribution", "bansos_penetration", "bansos_venn", "desil_venn"]
        welfare_available = [k for k in welfare_charts if k in charts]

        if welfare_available:
            elements.append(Paragraph("<b>2. Welfare & Social Aid Analysis</b>", self.styles['CustomH3']))
            elements.append(Spacer(1, 0.1 * inch))

            for i in range(0, len(welfare_available), 2):
                row = []
                for j in range(2):
                    if i + j < len(welfare_available):
                        key = welfare_available[i + j]
                        try:
                            img_io = io.BytesIO(charts[key])
                            row.append(Image(img_io, width=3.2*inch, height=2.8*inch))
                        except:
                            pass
                if row:
                    t = Table([row], colWidths=[page_width/2]*len(row))
                    t.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                    elements.append(t)
                    elements.append(Spacer(1, 0.2 * inch))

        # Section 3: Living Conditions & Assets
        asset_charts = ["housing_quality", "asset_ownership_heatmap", "sanitation_water"]
        for key in asset_charts:
            if key in charts:
                try:
                    elements.append(Paragraph("<b>3. Living Conditions & Assets</b>", self.styles['CustomH3']))
                    img_io = io.BytesIO(charts[key])
                    elements.append(Image(img_io, width=page_width * 0.85, height=3.0*inch))
                    elements.append(Spacer(1, 0.2 * inch))
                    break
                except:
                    pass

        if elements:
            elements.append(PageBreak())

        return elements

    def _create_fallback_table(self, title: str, keluarga_df: pd.DataFrame, anggota_df: pd.DataFrame) -> List:
        """Create simple fallback table when charts are unavailable."""
        if not REPORTLAB_AVAILABLE:
            return []

        data = [
            ["Metric", "Value"],
            ["Judul", html.escape(str(title))],
            ["Total Keluarga", str(len(keluarga_df))],
            ["Total Anggota", str(len(anggota_df))],
        ]

        table = Table(data, colWidths=[6*cm, 10*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#34495E")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#ECF0F1")]),
        ]))

        title_para = Paragraph(f"Fallback - {html.escape(str(title))}", self.styles.get('CustomH3', self.styles['Heading3']))
        return [title_para, Spacer(1, 0.1 * inch), table, Spacer(1, 0.2 * inch)]

    def _add_header_footer(self, canvas, doc):
        """Add premium header and footer to page with branding."""
        canvas.saveState()
        PAGE_SIZE = getattr(self, '_current_page_size', PAGE_SIZE_F4)

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
        logo_path = self.logo_path
        if os.path.exists(logo_path):
            try:
                canvas.drawImage(logo_path, 1.5*cm, PAGE_SIZE[1] - 2.2*cm,
                               width=4.5*cm, height=1.5*cm, mask='auto', preserveAspectRatio=True)
            except Exception as e:
                pass

        canvas.restoreState()

    def _pick_value(self, row: Any, keys: List[str], default: str = "-") -> str:
        """Pick first available value from keys."""
        for k in keys:
            try:
                if hasattr(row, '__getitem__'):
                    v = row.get(k) if isinstance(row, dict) else row[k]
                else:
                    v = getattr(row, k, None)
                if v is not None and str(v).strip() not in ('', 'nan', 'None', 'NaN'):
                    return str(v).strip()
            except:
                continue
        return default

    def _safe_text(self, value: Any) -> str:
        """Safely convert any value to text, handling None and NaN."""
        if value is None:
            return "-"
        if isinstance(value, float) and pd.isna(value):
            return "-"
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null", ""}:
            return "-"
        return text

    def _fmt_rupiah(self, value: Any) -> str:
        """Format currency values in Indonesian Rupiah format (Rp. 1.500.000)."""
        val_str = self._safe_text(value)
        if val_str == "-":
            return "-"
        try:
            clean_val = val_str.replace("Rp", "").replace(".", "").replace(",", "").replace(" ", "").strip()
            if not clean_val or clean_val == "0":
                return "-"
            num = float(clean_val)
            formatted = f"Rp. {int(num):,}".replace(",", ".")
            return formatted
        except:
            return val_str

    def _fmt_date(self, value: Any) -> str:
        """Format date values consistently."""
        val_str = self._safe_text(value)
        if val_str == "-":
            return "-"
        try:
            if "/" in val_str:
                parsed = datetime.strptime(val_str, "%d/%m/%Y")
                return parsed.strftime("%d-%m-%Y")
            elif "-" in val_str and len(val_str) == 10:
                return val_str
            return val_str
        except:
            return val_str

    def _map_desil(self, v) -> str:
        """Map desil values to standard labels."""
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

    def _load_csv(self, data: Optional[bytes]) -> pd.DataFrame:
        """Load CSV from bytes."""
        if not data:
            return pd.DataFrame()
        try:
            return pd.read_csv(io.BytesIO(data))
        except:
            return pd.DataFrame()
